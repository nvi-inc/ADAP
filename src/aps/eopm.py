import os
import logging
import math
from pathlib import Path

from utils import app
from utils.files import remove
from aps import solve
from aps.spool import read_spool
from aps.eop import EOP
from aps.eob import eob_to_eops


logger = logging.getLogger('aps')


class EOPM(EOP):
    # Initialize class with path
    def __init__(self, opa_config, initials, run_simul=False):
        super().__init__(opa_config, initials)
        self.run_simul = run_simul

        self.check_required_files(['EOPT_FILE', 'EOPM_FILE'], chk_write=True)

        options = app.Applications.APS.get('EOPM', dict(sites='/sgpvlbi/apriori/models/2023a_mod.sit',
                                                        min_dist=5000000))
        with open(options['sites']) as sit:
            self.sites = {line[:12].strip(): [(lambda x: float(x))(x) for x in line[12:].split()[:2]] for line in
                          sit.readlines() if not line.startswith('$')}
        self.min_dist = options['min_dist']

    # Get baseline and station lists
    def get_baselines(self, session, vgosdb):
        if not (spl := session.file_path('spl')).exists():
            self.add_error(f'Could not find {spl.name}')
            return [], []
        if not (spool := read_spool(path=spl, db_name=vgosdb.name)):
            self.add_error(f'Error reading {spl.name}')
            return [], []
        min_nbr_used_obs = int(self.get_opa_code('NUM_USED_MIN'))
        baselines, stations = [], []
        for run in spool.runs:
            if run.DB_NAME != session.db_name:
                self.add_error(f'Invalid DB_NAME {run.DB_NAME} in spool file {spl.name}')
                return [], []
            for name, stats in run.stats['baselines'].items():
                names = list(map(str.strip, name.split('|')))
                for sta_name in names:
                    if sta_name not in self.station_names:
                        self.add_error(f'No station code {sta_name} was found in the ns table.'
                                       f' Unknown station or bug?')
                        return False
                    stations.append(sta_name)
                if stats['used'] >= min_nbr_used_obs and \
                        math.dist(self.sites[names[0]], self.sites[names[1]]) > self.min_dist:
                    baselines.append(name)

        # Remove duplicates
        return sorted(list(dict.fromkeys(baselines))), sorted(list(dict.fromkeys(stations)))

    # Run SimpleSimul to get realistic UT1-TAI rms values
    def simulated_rms(self, vgosdb, spool):
        template, cfg = self.get_opa_path('SIMPLESIMUL'), self.get_control_filename()
        if not (slv := Path(os.environ.get('WORK_DIR'), f'{vgosdb.name}_{self.initials}.slv')).exists():
            self.add_error(f'SIMUL requested but {slv} does not exist!')
            return False
        words = {'@session_file@': str(slv)}
        if not self.make_control_file(template, cfg, words, header=False):
            self.add_error('SIMUL requested but could not make control file')
            return False
        ans = self.execute_command(f'SimpleSimul {cfg}', vgosdb.name)
        # Read UT1-TAI
        if 'Done' not in ans[-1] or 'Simulation results in' not in ans[-2]:
            self.add_error(f'SimpleSimul returned {ans[-1]}')
            return False
        path = ans[-2].split(':')[-1].strip()
        with open(path) as f:
            if not (values := [float(line.split()[-2].strip()) for line in f if 'UT1' in line]):
                self.add_error(f'No UT1 records in {path}')
                return False
            for run, rms in zip(spool.runs, values):
                logger.warning(f'UT1-TAI FE {run.UEOP[1]} replaced by RMS {rms:.2f}')
                run.UEOP[1] = rms

        remove(path)
        remove(slv)
        return True

    def get_eob_record(self, arc_line, session, vgosdb):
        # Make control file
        cnt, erp, template = self.get_control_filename(), self.get_opa_path('GEN_INPERP'), self.get_opa_path('EOPS_CNT')
        words = {'@erp_file@': f'{erp}  SPL  UT1S ', '@arc_line@': arc_line}
        if not self.make_control_file(template, cnt, words, header=True):
            self.add_error(f'could not make control file for eob record')
            return False, None
        # Call solve script
        solve.check_lock(self.initials)
        ans = self.execute_command(f'solve {self.initials} {cnt} silent', vgosdb.name)
        solve.remove_lock(self.initials)
        if not ans or not solve.check_status(self.initials):
            path = self.save_bad_solution('solve_', ans)
            self.add_error(f'Error running solve! Check control file {cnt} or output {path}')
            return False, None
        # Read spool file
        if not (spool := read_spool(initials=self.initials, db_name=vgosdb.name)):
            self.add_error(f'Error reading spool file SPLF{self.initials}')
            return False, None

        # Replace RMS for UT1 values if vgos session:
        if self.run_simul and not self.simulated_rms(vgosdb, spool):
            return False, None
        return True, spool.runs[0].make_eob_record(self.name2code, session.code, False)

    # Make control file and execute solve
    def execute(self, session, vgosdb):
        baselines, stations = self.get_baselines(session, vgosdb)
        if not baselines:
            self._warning = ' WARNING - no long baselines'
            return True
        wrapper, arc_lines = vgosdb.wrapper.name, []
        if not self.get_opa_code('EOPM_ONLY_SINGLE_BASELINE') == 'YES':
            arc_lines.append(self.format_arc_line(wrapper, session.code))
        # Add exclusions to arc_line
        if len(stations) > 2:
            for name in baselines:
                excluded = list(set(stations) - set(name.split('|')))
                fmt = '{:100s} STA_EXCLUDE {:2d} ' + '{:8s}  ' * len(excluded) + ' ! {}'
                arc_lines.append(fmt.format(wrapper, len(excluded), *excluded, session.code))

        records = []
        for arc_line in arc_lines:
            ok, val = self.get_eob_record(arc_line, session, vgosdb)
            if not ok:
                return False
            records.append(val)

        # Update eob
        eob = self.get_opa_path('EOPT_FILE')
        prefix, suffix = os.path.splitext(os.path.basename(eob))
        tpath = self.get_tmp_file(prefix+'_', suffix)
        self.update_eop_file(tpath, eob, records)
        if not self.update_global_file(tpath, eob):
            return False

        # Update eopm file
        eopm = self.get_opa_path('EOPM_FILE')
        prefix, suffix = os.path.splitext(os.path.basename(eopm))
        tpath = self.get_tmp_file(prefix+'_', suffix)
        # Create tmp file and update global file
        eob_to_eops(eob, tpath, is_eopm=True)
        return self.update_global_file(tpath, eopm)
