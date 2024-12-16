import re
import sys
from pathlib import Path
from datetime import datetime
import tempfile
import shutil
import signal
import os
import time
import traceback
import subprocess
from operator import attrgetter, itemgetter

from utils import app, readDICT, nc
from utils.files import remove
from utils.mail import build_message, send_message
from utils.servers import get_server, get_config_item, CORRELATOR
from vgosdb.compress import VGOStgz
from vgosdb import VGOSdb, vgosdb_folder, get_db_name
from vgosdb.nusolve import get_nuSolve_info
from aps import APS, submit, get_aps_process, spool
from ivsdb import IVSdata


# Class to control vgosDB download from correlator
class VGOSDBController:

    # Initialize variables using config file
    def __init__(self, user=None):

        self.user = user
        self.agency = self.notifications = self.nusolveApps = self.auto = self.lastmod = self.origin = None
        self.vgosdb = self.moved_folder = None
        self.same_correlator_data = False
        self.check_control_file()

        self.save_corr_report = app.args.corr if hasattr(app.args, 'corr') else True

    # Print message. Could be overwritten by Broker class
    def info(self, msg):
        print(type(self).__name__, msg)

    # Print message. Could be overwritten by Broker class
    def warning(self, msg):
        print(type(self).__name__, msg)

    # Sleep for 1 second. Could be overwritten by Broker class to use pika sleep.
    def sleep_1sec(self):
        time.sleep(1)
        print(type(self).__name__, 'end of sleep')

    # Send notification to default watchdog. Could be overwritten by Broker class
    def notify(self, msg):
        self.warning(msg)
        app.notify('VGOS DB', msg)

    def check_control_file(self):
        # Read configuration file every time in case there was some changes.
        self.lastmod, info = app.load_control_file(name=app.ControlFiles.VGOSdb, lastmod=self.lastmod)
        if info:
            self.nusolveApps = info['nuSolve']
            self.auto = info.get('Auto', {})
            self.save_corr_report = info['Options'].get('save_correlator_report', True)
            self.notifications = info['Notifications']
            # Read agency code
            conf = readDICT(os.path.expanduser(info['Agency']['file']))
            self.agency = conf[info['Agency']['keys'][0]][info['Agency']['keys'][1]]

    # Send message that vgosDB is ready
    def send_ready_email(self, action, err):
        if app.args.no_mail:
            return  # Do not send email

        summary = self.vgosdb.summary(self.agency)
        vtype = 'problem' if app.args.test else self.vgosdb.type
        vtype == 'intensive' if vtype == 'intensives' else vtype
        if vtype not in self.notifications or vtype == VGOSdb.Unknown:
            self.notify(f'{self.vgosdb.name} type was detected as {self.vgosdb.type}')
            vtype = 'problem'
        if self.vgosdb.session.lower() != self.vgosdb.code.lower():
            err.append(f'\n\n*** WARNING: Head.nc has session name ({self.vgosdb.code} different than master'
                       f' ({self.vgosdb.session})***\n')
        sender, recipients = self.notifications['sender'], self.notifications[vtype]
        sender = sender.replace('watchdog', self.user) if self.user else sender

        title = f'{self.vgosdb.name} ({self.vgosdb.code}) has been {action} and is ready for processing'
        errs = '\n'.join(err)
        message = f'{self.vgosdb.name} from {self.origin} is available at {self.vgosdb.folder}\n\n{summary}\n{errs}'
        msg = build_message(sender, recipients, title, text=message)
        send_message(self.notifications['server'], msg)

    def send_copy_email(self, comments):
        if app.args.no_mail:
            return  # Do not send email

        vtype = 'problem' if app.args.test else self.vgosdb.type
        vtype == 'intensive' if vtype == 'intensives' else vtype
        if vtype not in self.notifications or vtype == VGOSdb.Unknown:
            self.notify(f'{self.vgosdb.name} type was detected as {self.vgosdb.type}')
            vtype = 'problem'
        sender, recipients = self.notifications['sender'], self.notifications[vtype]
        recipients.extend(self.notifications['problem'])
        sender = sender.replace('watchdog', self.user) if self.user else sender

        title = f'{self.vgosdb.name} ({self.vgosdb.code}) has same correlated data'
        message = f'{self.vgosdb.name} from {self.origin} ' \
                  f'was downloaded in {self.vgosdb.folder}\n' \
                  f'Head.nc has new time tag but correlated data are the same\n' \
                  f'History, wrappers and .nc files were copied from {self.moved_folder}\n\n{comments}'

        msg = build_message(sender, recipients, title, text=message)
        send_message(self.notifications['server'], msg)

    # Send message that vgosDB is ready
    def send_auto_processing_email(self, summary, err):
        self.info(f'{self.vgosdb.name} - send auto processing email')
        vtype = 'intensive' if self.vgosdb.type == 'intensives' else self.vgosdb.type
        if vtype not in self.notifications or vtype == VGOSdb.Unknown:
            self.notify(f'{self.vgosdb.name} type was detected as {self.vgosdb.type}')
            vtype = 'problem'
        if self.vgosdb.session.lower() != self.vgosdb.code.lower():
            err.append(f'\n\n*** WARNING: Head.nc has session name ({self.vgosdb.code} different than master'
                       f' ({self.vgosdb.session})***\n')
        sender, recipients = self.notifications['sender'], self.notifications[vtype]
        sender = sender.replace('watchdog', self.user) if self.user else sender

        title = f'{self.vgosdb.name} ({self.vgosdb.code}) has been automatically processed{" [PROBLEM]" if err else ""}'
        errs = f"PROBLEMS\n--------\n{err}" if err else ''
        message = f'{self.vgosdb.name} from {self.origin} has been processed in {self.vgosdb.folder}\n\n{summary}{errs}'
        msg = build_message(sender, recipients, title, text=message)
        ret = send_message(self.notifications['server'], msg)
        self.info(f'{self.vgosdb.name} - send email: {ret if ret else "ok"}')

    # Download vgosDB file
    def download(self, center, rpath):
        errors = []
        # Do it few times to avoid empty file due to early detection.
        for nbr_tries in range(5):
            # Make unique tmp file for this
            lpath = tempfile.NamedTemporaryFile(delete=False).name
            with get_server(CORRELATOR, center) as server:
                ok, info = server.download(rpath, lpath)
                if not ok or not os.stat(lpath).st_size:
                    err = f'Download failed {ok} - [{info}]'
                    self.warning(err)
                    errors.append(err)
                    remove(lpath)
                    time.sleep(1)
                    continue
                return lpath
        # Error downloading this vgosDb
        err = '\n'.join(errors)
        self.notify(f'{err}\n{str(traceback.format_exc())}')
        return None

    # Test if vgosDB already exists and
    @staticmethod
    def is_new(db_name, lpath, folder):
        tgz = VGOStgz(db_name, lpath)
        create_time, err = tgz.get_create_time()
        if not create_time:
            return False, err
        if not os.path.isdir(folder):
            return True, 'downloaded'  # Folder does not exist
        if not os.access(folder, os.R_OK):
            return False, 'No privileges to read folder'  # Folder is protected
        if create_time > VGOSdb(folder).create_time:
            return True, 'updated'
        return False, 'Created time same or older'

    @staticmethod
    def is_used_by_aps(db_name):
        # Get ses_id from database
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            ses_id = dbase.get_db_session_code(db_name)
            for pid, code in get_aps_process():
                if code == db_name or code == ses_id:
                    break
                # Check if code is initial for spool file
                if (spl := spool.read_spool(initials=code)) and dbase.get_db_session_code(spl.runs[0].DB_NAME) == ses_id:
                    break
            else:
                return False
        os.kill(pid, signal.SIGUSR1)
        return True

    # Move new vgosDb to appropriate folder
    def extract_vgosdb(self, db_name, lpath, folder):
        if os.path.isdir(folder):
            self.moved_folder = self.rename_folder(folder, 'p')
            if os.path.isdir(folder):  # Not able to move folder
                return False
            if (mv := Path('/sgpvlbi/progs/scripts/mv-vgosdb')).exists():
                try:
                    app.exec_and_wait(f'{str(mv)} {folder} {self.moved_folder}')
                except Exception as exc:
                    self.notify(f'mv-vgosdb problem\n{str(exc)}')
        try:
            # Extract compress file to folder
            tgz = VGOStgz(db_name, lpath)
            tgz.extract(folder)
            return True
        except:
            return False

    # Get the name without extensions (for tar.gz files)
    @staticmethod
    def validate_db_name(center, name):
        basename = name.replace(''.join(Path(name).suffixes), '')
        db_name = get_db_name(name)['name']
        # Check if what we expect from correlator site
        if file_name := get_config_item(CORRELATOR, center, 'file_name'):
            expected = os.path.basename(file_name.format(year='', db_name=db_name))
            if basename != expected.replace(''.join(Path(expected).suffixes), ''):
                return None

        return db_name

    # Process information regarding new file
    def process(self, center, name, rpath, reject_old=False):

        self.origin = center.upper()

        self.check_control_file()

        # Make sure the filename and db_name agree.
        basename = os.path.basename(rpath)
        if not (db_name := self.validate_db_name(center, basename)):
            self.info(f'{basename} from {center} has not been downloaded [Not accepted name]')
            return True

        folder = vgosdb_folder(db_name)
        if reject_old and db_name[:8].isdigit() and db_name[:4] < '2023':
            self.info(f'{basename} from {center} has not been downloaded [Before 2023]')
            return True

        # Make year folder if it does not exist
        os.makedirs(os.path.dirname(folder), exist_ok=True)
        # Download to temp folder
        if not (lpath := self.download(center, rpath)):
            return False  # Download failed
        ok, msg = self.is_new(db_name, lpath, folder)

        if not ok:
            self.warning(f'{db_name} from {center} not download. [{msg}]')
        # elif self.is_used_by_aps(db_name):
        #    self.warning(f'APS is processing {db_name}')
        #    remove(lpath)
        #    return False
        elif self.extract_vgosdb(db_name, lpath, folder):
            try:
                self.processDB(folder, msg)
            except Exception as err:
                self.notify(f'{name} {str(err)}\n{str(traceback.format_exc())}')
        remove(lpath)
        return True

    def process_file(self, db_name, path):

        self.origin = path

        folder = vgosdb_folder(db_name)
        ok, msg = self.is_new(db_name, path, folder)

        if self.extract_vgosdb(db_name, path, folder):
            try:
                self.processDB(folder, msg)
            except Exception as err:
                self.notify(f'{db_name} {str(err)}\n{str(traceback.format_exc())}')
        return True

    def use_last_wrapper(self):
        # Check if one of the station has Cal-Cable_kPcmt.nc file
        for name in self.vgosdb.station_list:
            if Path(self.vgosdb.folder, name, 'Cal-Cable_kPcmt.nc').exists():
                return True
        # Check if session has VLBA stations and no logs for them
        url, tunnel = app.get_dbase_info()
        vlba = app.VLBA.stations
        with IVSdata(url, tunnel) as dbase:
            names = dbase.get_station_name_dict()
            session = dbase.get_session(self.vgosdb.session)
            for name in self.vgosdb.station_list:
                if (sta_id := names[name]) in vlba and not session.log_path(sta_id).exists():
                    if Path(self.vgosdb.folder, name, 'Met.nc').exists():
                        return True
        return False

    # Process applications required by nuSolve
    def processDB(self, folder, action=None):
        if not folder or (hasattr(app.args, 'no_processing') and app.args.no_processing):
            return  # Nothing to do

        # Check if correlator data are same
        self.vgosdb = VGOSdb(folder)
        # Save correlator report
        if not app.args.test and self.save_corr_report:
            try:
                if name := self.vgosdb.save_correlator_report():
                    self.info(f'{name} saved to {self.vgosdb.code}')
            except Exception as exc:
                self.warning(f'Could not save {name}\n{str(exc)}')
                pass

        try:
            if self.check_correlator_data():
                self.warning(f'vgosDB correlator data same as {self.moved_folder}')
                return
        except Exception as exc:
            self.notify(f'{str(exc)}\n{str(traceback.format_exc())}')
            return

        if self.vgosdb.get_last_wrapper(self.agency):
            self.warning(f'vgosDB already processed by {self.agency}')
            return  # This vgosDb has already been process by this agency
        err = []
        wrapper = self.vgosdb.get_oldest_wrapper()
        wrapper = wrapper if (skip := self.use_last_wrapper()) else self.vgosdb.get_v001_wrapper()
        for app_info in self.nusolveApps:
            # Check if this processing is done for VGOS sessions
            if skip and not app_info.get('processVGOS', False):
                continue
            # Execute app
            path = path if (path := shutil.which(app_info['name'])) else app_info['path']
            cmd, ans, errors = self.exec_app(path, app_info.get(self.vgosdb.type, ''), wrapper)
            # Test if application name in last wrapper
            wrapper = self.vgosdb.get_last_wrapper(self.agency, reload=True)
            if not wrapper or app_info['name'] not in wrapper.processes:
                err.extend([f'\n{cmd} failed!\n', ans, errors])
                for line in ans.splitlines()+errors.splitlines():
                    self.warning(f'{self.vgosdb.name} {line}')
                break

        # Check if $EXPER is correct
        if skd_msg := self.check_sched():
            err.append(skd_msg)

        if options := self.auto.get(self.vgosdb.type, None):
            try:
                self.auto_analysis(options, skd_msg)
            except Exception as exc:
                self.notify(f'{str(exc)}\n{str(traceback.format_exc())}')
        else:
            self.send_ready_email(action, err)

    def check_sched(self):
        ses_id = self.vgosdb.session.upper()
        url, tunnel = app.get_dbase_info()
        with IVSdata(url, tunnel) as dbase:
            if session := dbase.get_session(ses_id):
                if (path := session.file_path('skd')).exists():
                    with open(path) as f:
                        lines = f.readlines()
                    if lines[0].split()[1] != ses_id:
                        shutil.move(path, f'{path}.ori')
                        with open(path, 'w') as f:
                            f.write(f'$EXPER {ses_id}\n{"".join(lines[1:])}')
                        return f'{lines[0].strip()} was changed to {ses_id} in {path.name}'
        return ''

    def execute_nuSolve(self, cmd):
        try:
            prc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            out, err = prc.communicate()
            key = 'Number of total obs'
            listing = out[out.find(key):] + err[err.find(key):]
            ok = prc.returncode == 0
            return (ok, listing) if ok else (ok, f'{out}\n{err}\nUSER {os.environ.get("USER")}\n{cmd}'
                                                 f'\n{os.environ.get("PATH", "PATH NOT DEFINED")}'
                                                 f'\nnuSolve path:{shutil.which("nuSolve")}'
                                                 f'\nsolve path:{shutil.which("solve")}'
                                                 f'\n{os.stat("/sgpvlbi/progs/nusolve/bin/nuSolve")}')
        except Exception as err:
            return False, f"ERROR: {str(err)}"

    def auto_analysis(self, options, warning=None):

        dbname = self.vgosdb.name
        self.info(f'{dbname} - auto analyse')
        analyst = options['analyst']
        options = options[os.environ.get('NUSOLVE_AUTO', 'ADAP')]
        summary = [warning, ''] if warning else ['']
        summary.extend(['nuSolve solution summary', '-'*24, ''])
        # Run nuSolve
        ok, nusolve_ans = self.execute_nuSolve(options['cmd'].format(db_name=dbname))
        if ok:
            self.info(f'{dbname} - nuSolve ok')
            summary.append(nusolve_ans)
            summary.append('')
            nusolve = get_nuSolve_info(self.vgosdb.get_last_wrapper(self.agency, reload=True))
            initials = nusolve['initials']
            self.info(f'{dbname} - aps {initials}')
            aps = APS(initials)
            if not aps.has_errors:
                self.info(f'{dbname} - aps {aps.db_name} {aps.session.code} {aps.session.type}')
                # Run all post nuSolve required processes
                self.info(f'{dbname} - aps (A) {list(aps.processing.Actions.keys())}')
                self.info(f'{dbname} - aps (A) {[info["required"] for info in aps.processing.Actions.values()]}')
                self.info(f'{dbname} - aps (S) {list(aps.processing.Submissions.keys())}')
                self.info(f'{dbname} - aps (S) {[info["required"] for info in aps.processing.Submissions.values()]}')
                self.info(f'{dbname} - aps (ERR) {aps.errors}')
                for action, info in aps.processing.Actions.items():
                    if info['required']:
                        self.info(f'{dbname} - aps {action}')
                        if not aps.run_process(action, options['initials'], auto=True):
                            self.warning(f'{dbname} - aps {action} failed')
                            break
                        else:
                            summary.append(f'{action} done {aps.processing.done(action)}')
                else:  # loop not stopped by break. Submit files
                    for submission, info in aps.processing.Submissions.items():
                        if info['required']:
                            self.info(f'{dbname} - aps {submission}')
                            if ans := aps.submit_results(submission.replace('SUBMIT-', '')):
                                for file_name in ans:
                                    aps.logit(f'{file_name} will be uploaded later')
                                    summary.append(f'{submission} {aps.processing.done(submission)} - '
                                                   f'{file_name} will be uploaded later')
                            if aps.has_errors:
                                break
                            else:
                                for file in submit.get_last_submission():
                                    file_name = os.path.basename(file)
                                    aps.logit(f'{file_name} submitted')
                                    self.info(f'{dbname} - submitted {file_name}')
                                    summary.append(f'{submission}: {aps.processing.done(submission)}'
                                                   f' - {file_name} submitted')
                    else:  # loop not stopped by break. Generate analysis report
                        self.info(f'{dbname} - aps make analysis report')
                        is_ivs = aps.processing.check_agency()
                        ok, txt = aps.make_analysis_report(is_ivs, [], [], [], auto=analyst)
                        if ok:
                            aps.processing.TempReport = tempfile.NamedTemporaryFile(
                                prefix='{}_report_'.format(aps.session.code), suffix='.txt', delete=False).name
                            with open(aps.processing.TempReport, 'w') as out:
                                out.write(txt)
                            aps.logit('submit analysis report and spoolfile')
                            ans = aps.submit_report(is_ivs)
                            if not aps.has_errors:
                                summary.append(f'{aps.processing.Reports[-1]} submitted')
                                summary.append(f'{aps.processing.SpoolFiles[-1]} submitted')
                                for file_name in ans:
                                    aps.logit(f'{file_name} will be uploaded later')
                                # Send email to IVS
                                if msg := aps.send_analyst_email('last'):
                                    aps.errors(f'Failed sending report {msg}')
                                else:
                                    summary.append('Analysis report sent to IVS mail')

        self.info(f'{dbname} - end of aps')
        err = aps.errors if ok else nusolve_ans
        self.send_auto_processing_email("\n".join(summary), err)

    # Execute pre-nuSolve applications
    def exec_app(self, path, option, wrapper):
        # Clean option and make command
        if option.startswith('-'):
            option = ' ' + option
        cmd = f'{path}{option} {wrapper.path}'
        # Execute command
        ans, err = app.exec_and_wait(cmd)
        return cmd, ans, err

    # Rename folder in case it already exist
    @staticmethod
    def rename_folder(folder, prefix):
        if os.path.exists(folder):
            for index in range(1, 10):
                new_folder = f'{folder}.{prefix}{index}'
                if not os.path.isdir(new_folder):
                    os.renames(folder, new_folder)
                    return new_folder
        return None

    # Send warning when it fails
    def failed(self, msg):
        self.warning(msg)
        return False

    def check_correlator_data(self):
        if not self.moved_folder:
            return False
        # Check if all files created in wrapper V001 are the same
        db_p = VGOSdb(self.moved_folder)
        for path in self.vgosdb.get_v001_wrapper().get_files('.nc'):
            if not nc.same(Path(self.vgosdb.folder, path), Path(db_p.folder, path)):
                return False
        # Check that our agency has already processed it
        if not (last := db_p.get_last_wrapper(self.agency)) or 'nuSolve' not in last.processes:
            return False
        # Check if we are IVS analysis center
        ac = app.Applications.APS['analysis_center']
        dbase = app.get_dbase()
        is_ivs = (ac.casefold() == dbase.get_session(dbase.get_db_session_code(self.vgosdb.name)).analysis_center)
        # Remove all files from our agency in new vgosdb
        self.clean_agency_files(is_ivs)
        # Get agency wrappers from old vgosdb
        wrappers = sorted([w for w in db_p.wrappers if w.agency == self.agency], key=attrgetter('name'))
        processes = sorted(list(wrappers[0].processes.values()), key=itemgetter('runtimetag'))
        in_wrp_name = db_p.get_wrapper(processes[-1].get('inputwrapper')).name
        if not (input_wrp := self.vgosdb.get_wrapper(in_wrp_name)):
            raise Exception(f'Did not find {in_wrp_name} in {self.vgosdb.name}')
        nc_files = list(set(list(wrappers[-1].get_files('.nc'))).difference(input_wrp.get_files('.nc')))
        # Copy nc files and keep old name new name relation in dict.
        nc_dict = {file: self.copy_nc(db_p.folder, Path(file)) for file in nc_files}
        # Copy history files and wrappers
        name_dict, first_time_tag = {}, None
        for wrapper in wrappers:
            process = sorted(list(wrapper.processes.values()), key=itemgetter('runtimetag'))[-1]
            in_wrp_name = name_dict.get(name := db_p.get_wrapper(process.get('inputwrapper')).name, name)
            if not (in_wrp := Path(self.vgosdb.folder, in_wrp_name)).exists():
                raise Exception(f'Did not find input wrapper {in_wrp_name} for {wrapper.name}')
            hist_ori, hist_dst, hist_time = self.copy_history_file(db_p, process, first_time_tag)
            name_dict[hist_ori] = hist_dst
            first_time_tag = first_time_tag if first_time_tag else hist_time
            name_dict[wrapper.name] = self.copy_wrapper(db_p, wrapper.name, nc_dict, name_dict, ac, first_time_tag)

        self.info(f'{self.vgosdb.name} correlator data were same. Files copied form {Path(db_p.folder).name}')
        comments = [f'{self.agency} is {"" if is_ivs else "not "}the IVS analysis center']
        # Submit database if IVS analysis center
        if is_ivs:
            aps = APS(self.vgosdb.name)
            if not aps.has_errors:
                ans = aps.submit_results('DB')
                self.info(msg := f'{self.vgosdb.name}.tgz '
                                 f'{"created but will be submitted later" if ans else "submitted"}')
                comments.append(msg)
            else:
                comments.append(f'**** Could not submit {self.vgosdb.name} [{aps.errors}]')

        self.send_copy_email('\n'.join(comments))
        return True

    def copy_nc(self, ori, relpath):
        if Path(dst := self.vgosdb.folder, path := relpath).exists():
            sub_folder, name, version = relpath.parent, relpath.stem, 1
            if match := re.match(relpath.stem, r'(?P<name>.*)_V(?P<version>\d{3}).nc'):
                name, version = match['name'], int(match['version']) + 1
            while Path(dst, path := Path(sub_folder, f'{name}_V{version:03d}.nc')).exists():
                version += 1
        (new_path := Path(dst, path)).parent.mkdir(exist_ok=True)
        shutil.copyfile(Path(ori, relpath), new_path)
        nc.update_create_time(new_path, datetime.utcnow())
        return str(path)

    def copy_wrapper(self, db_p, name, nc_dict, name_dict, ac, first_time_tag):
        src, dst = Path(db_p.folder, name), Path(self.vgosdb.folder, name)
        with open(src) as f_in, open(dst, 'w') as f_out:
            fmt = 'RunTimeTag %Y/%m/%d %H:%M:%S UTC'
            # Split Header, History and Content parts of the wrapper
            match = next(re.finditer(r'(VERSION.*)(Begin History.*End History.)(.*)', f_in.read(), re.DOTALL))
            header, history, content = match.group(1), match.group(2), match.group(3)
            print(header, end='', file=f_out)
            # Replace RunTimeTag, History and InputWrapper in History records
            is_nasa = False
            for line in history.splitlines():
                key, _, info = line.partition(' ')
                key, info = key.casefold(), info.strip()
                if key == 'createdby' and ac in info:
                    is_nasa = True
                elif key in ('history', 'inputwrapper') and is_nasa:
                    line = line.replace(info, name_dict.get(info, info))
                elif key == 'runtimetag' and is_nasa:
                    time_tag = datetime.strptime(line, fmt)
                    line = f'{datetime.utcnow() + (time_tag - first_time_tag):{fmt}}'
                elif key.startswith('end'):
                    is_nasa = False
                print(line, file=f_out)
            # Change name of nc files if required
            names = None
            for line in content.splitlines():
                key, _, info = line.partition(' ')
                if key.casefold().startswith('default_dir'):
                    names = {Path(f).name: Path(t).name for f, t in nc_dict.items() if f.startswith(info)}
                elif names and line.endswith('.nc'):
                    line = names.get(line.strip(), line)
                elif key.casefold().startswith('end'):
                    names = None
                print(line, file=f_out)

        return dst.name

    def copy_history_file(self, db_p, process, time_tag=None):
        # Copy history file to new folder
        ori = Path(db_p.folder, process['default_dir'], process['history'])
        old_name = filename = str(Path(process['default_dir'], process['history']))
        if (dst := Path(self.vgosdb.folder, filename)).exists():
            parts = process['history'].split('_')
            filename = f'{self.vgosdb.name}_{parts[1]}_i{self.agency}_{parts[-1]}'
            dst = Path(self.vgosdb.folder, process['default_dir'], filename)
        shutil.copyfile(ori, dst)
        prefix = 'Copied from ' if 'nuSolve' in dst.name else 'w copied from '
        # Add comment at end of file
        pattern, fmt = re.compile('TIMETAG .{19} UTC', re.DOTALL), 'TIMETAG %Y/%m/%d %H:%M:%S UTC'
        with open(dst, "r+") as file:
            content = file.read()
            file.seek(0)
            file.truncate()
            created = datetime.strptime(pattern.findall(content)[0], fmt)
            time_tag = time_tag if time_tag else created
            for line in content.splitlines():
                if line.startswith('TIMETAG'):
                    line = f'{datetime.utcnow() + (datetime.strptime(line, fmt) - time_tag):{fmt}}'
                print(line, file=file)
            print(f'{prefix}{ori} with {created:{fmt}}', file=file)
        return old_name, filename, time_tag

    def clean_agency_files(self, is_ivs):
        # Get first agency wrapper from vgosdb
        agencies = {self.agency, 'IVS'} if is_ivs else {self.agency}
        if not (wrappers := [w for w in self.vgosdb.wrappers if w.agency in agencies]):
            return
        wrappers.sort(key=attrgetter('name'))
        processes = sorted(list(wrappers[0].processes.values()), key=itemgetter('runtimetag'), reverse=True)
        file_types = ('.nc', '.hist')
        if in_wrp := self.vgosdb.get_wrapper(processes[0].get('inputwrapper')):
            previous = {ftype: in_wrp.get_files(ftype) for ftype in file_types}
            for wrapper in wrappers:
                for ftype in file_types:
                    for file in list(set(wrapper.get_files(ftype)).difference(previous[ftype])):
                        if (path := Path(self.vgosdb.folder, file)).exists():
                            os.remove(path)
                os.remove(Path(self.vgosdb.folder, wrapper.name))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Logger for ADAP software.' )

    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--db', help='database name', default='ivscc', required=False)
    parser.add_argument('-m', '--no_mail', help='no email', action='store_true')
    parser.add_argument('-t', '--test', help='test mode', action='store_true')
    parser.add_argument('final')
    parser.add_argument('old')

    args = app.init(parser.parse_args())

    ctrl = VGOSDBController()
    ctrl.vgosdb = VGOSdb(args.final)
    ctrl.moved_folder = args.old
    try:
        if ctrl.check_correlator_data():
            print(f'vgosDB correlator data same as {ctrl.moved_folder}')
        else:
            print('New vgosDB')
    except Exception as exc:
        ctrl.notify(f'{str(exc)}\n{str(traceback.format_exc())}')
