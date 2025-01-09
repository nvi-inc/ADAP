import os

from datetime import timedelta
from io import StringIO
from collections import defaultdict

from schedule import get_schedule

Comments = ('Problems:', 'Parameterization comments:', 'Other comments:')


# Class use to update GLO_ARC_FILE
class STDreport:
    # Initialize class with path
    def __init__(self, session, vgosdb, spool):
        self.session, self.vgosdb, self.spool = session, vgosdb, spool
        self.f_out, self.extra_comment = None, []
        if not (sched := get_schedule(self.session)):
            self.extra_comment = [f'{self.session.code} has no schedule file']
        elif sched.session_code.lower() != self.session.code.lower():
            self.extra_comment = [f'{os.path.basename(sched.path)} has invalid record $EXPER {sched.session_code}']
        if self.extra_comment:
            self.extra_comment.extend(['Therefore the source breakdown at the end of the summary has been deleted.',
                                       'The reported information is based on the nuSolve spoolfile, which is based',
                                       'on the correlated data.'])
            sched = None
        else:
            sched.stations['removed'] = self.session.removed
        self.schedule = sched
        self.vgosdb.statistics()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if not self.f_out:
            self.f_out = StringIO()

    def close(self):
        pass

    def get_text(self):
        return self.f_out.getvalue()

    def write(self, line, center=False, before=0, after=0):
        if before:
            self.f_out.write('\n' * before)
        self.f_out.write(line.center(80) if center else line)
        self.f_out.write('\n' * (after + 1))

    def write_lines(self, title, lines, write_empty=False, before=0, after=0):
        if not lines:
            if write_empty:
                self.write(f'{title} None.', before=before, after=after)
            return
        if before:
            self.write('', before=before-1)
        for line in lines:
            self.write(f'{title} {line}')
            title = ' ' * len(title)
        if after:
            self.write('' * after)

    def write_header(self, analysis_center, is_ivs_report=False):
        analysis_center = 'IVS' if is_ivs_report else analysis_center.upper()
        self.write(f'       {analysis_center} Analysis Report for {self.session.code.upper()} ({self.session.db_name})')
        if is_ivs_report:
            first_line = '       This report is the official IVS analysis report that corresponds to'
        else:
            first_line = '       This report is a contributed analysis report that does not correspond to'
        self.write(first_line)
        self.write('       the database maintained by the IVS Data Centers for this session.')

    def write_analysts_info(self, analysis_center, nuSolve, auto=None):
        analyst = auto if auto else nuSolve.get('analyst', 'N/A')
        analysis_center_name = nuSolve.get('agency', 'N/A')
        self.write(f'   (Analyzed by {analyst}, {analysis_center_name}.')
        self.write(f'    Spoolfile source: {"automated " if auto else ""}nuSolve solutions.)', after=1)

        self.write(f'Session scheduled at {self.session.operations_center.upper()}, '
                   f'correlated at {self.vgosdb.correlator.upper()} and '
                   f'analyzed at {analysis_center.upper()}', after=1)

    def write_comments(self, key, comments, in_line=None):
        if key == 0:
            comments = self.extra_comment + comments
        # Write analysts comments
        self.write_lines(Comments[key], comments, write_empty=True, after=1)

    def get_total_recoverable(self):
        stats, total_recoverable = self.spool.runs[0].stats['stations'], 0
        if self.schedule:
            for name in self.schedule.stations['names'].keys():
                if self.schedule.stations['names'][name]['scheduled_obs'] > 0 and name in self.vgosdb.stats:
                    total_recoverable += stats[name]['recov'] if name in stats else 0
        else:
            for data in stats.values():
                total_recoverable += data['recov']
        return int(total_recoverable / 2)

    def write_warnings(self):
        if not self.schedule:
            self.write('***WARNING:  Schedule file not valid for this spoolfile***', before=1, after=1)
            return

        # Check if stations in spool file are not in schedule or have been removed
        not_scheduled, removed_but_there = [], []
        please_check = '***          Please also check the master schedule file ' \
                       'to make sure it is correct for this session.***'
        for name in self.spool.runs[0].stats['stations'].keys():
            if name not in self.schedule.stations['names']:
                not_scheduled.append(name)
            if name in self.schedule.stations['removed']:
                removed_but_there.append(name)
        if not_scheduled:
            self.write(f'***WARNING:  Some station(s) are in spoolfile but not scheduled: {" ".join(not_scheduled)}***',
                       before=1)
            self.write(please_check, after=1)
        if removed_but_there:
            self.write('***WARNING:  Some station(s) were removed from the master schedule entry for this session,',
                       before=1)
            self.write(f'             but they had usable data, according '
                       f'to the Solve/nuSolve spoolfile: {" ".join(not_scheduled)}***')
            self.write(please_check, after=1)
        # Check for sources in spool but not in schedule
        if not_scheduled := [name for name in self.spool.runs[0].stats['sources'].keys()
                             if name not in self.schedule.sources]:
            self.write(f'***WARNING:  Some source(s) are in the spoolfile or the database '
                       f'but not in the schedule: {" ".join(not_scheduled)}***', before=1)

    def write_stats(self):
        stats = self.spool.runs[0].stats['session']
        recovered = self.get_total_recoverable()
        scheduled = str(self.schedule.scheduled_obs) if self.schedule else '?'*len(str(self.vgosdb.correlated))
        # Write session statistics
        self.write('-' * 41, after=1)
        self.write('Session statistics', after=1)
        self.write_lines('  Observations:',
                         [f'{scheduled:>8s} scheduled', f'{self.vgosdb.correlated:8d} correlated (in database)',
                          f'{recovered:8d} recoverable (usable)', f'{stats["used"]:8d} used'])
        wrms = f'{self.spool.runs[0].WRMS:13.3f} ps' if self.spool else 'N/A'
        self.write(f'  Session fit: {wrms}', after=1)

    def write_station_performance(self):
        if not self.schedule:
            return
        # Write header
        self.write('-' * 41, after=1)
        self.write('Station Performance', after=1)
        self.write(f'{"Number of Observations":>49s}', after=1)
        self.write(f'{"Scheduled   Recoverable*        Used   % of scheduled":>73s}')
        self.write(f'{"obs used":>70}', after=1)

        # Write station stats
        total_scheduled = total_recoverable = total_used = 0
        stats = self.spool.runs[0].stats['stations']
        for name in sorted(self.schedule.stations['names'].keys()):
            scheduled = self.schedule.stations['names'][name]['scheduled_obs']
            total_scheduled += scheduled
            if scheduled == 0:
                continue
            if name not in self.vgosdb.stats:
                reason = 'MISSED' if self.schedule and name in self.schedule.missed else 'NOT CORR'
                self.write(f'  {name:<8s} {scheduled:18d} {reason:>13s} {reason:>12s} {0.0:11.1f}%')
            elif name not in stats:
                reason = 'DESELECTED'
                self.write(f'  {name:<8s} {scheduled:18d} {reason:>13s} {reason:>12s} {0.0:11.1f}%')
            else:
                obs = stats[name]
                total_recoverable += obs['recov']
                total_used += obs['used']
                used = 'NOT USED' if obs['used'] == 0 else f'{obs["used"]:d}'
                percent = obs['used'] / scheduled * 100
                self.write(f'  {name:<8s} {scheduled:18d} {obs["recov"]:13d} {used:>12s} {percent:11.1f}%')

        self.write('  ---------------   ---------   -----------    ---------       ------')
        percent = total_used / total_scheduled * 100
        self.write(f'  Station Total**   {int(total_scheduled / 2):9d} {int(total_recoverable / 2):13d} '
                   f'{int(total_used / 2):12d} {percent:11.1f}%')
        # Write footer
        self.write('  * Recoverable: can be included in the solution.', before=1)
        self.write('  ** Total includes distinct observations only.')
        self.write_lines('  MISSED:   ', ['Station was scheduled, but it did not observe.'], before=1)
        self.write_lines('  NOT CORR: ', ['Station was scheduled but not correlated.'])
        self.write_lines('  NOT USED: ', ['Usable data was generated for this station,',
                                          'but the analyst rejected it all.'], after=1)

    def write_source_performance(self):
        if not self.schedule:
            return
        # Write header
        self.write('-' * 41, after=1)
        self.write('Source Performance', after=1)
        self.write(f'{"Number of Observations":>42s}', after=1)
        self.write(f'{"Scheduled  Correlated*    Used      % of scheduled":>66s}')
        self.write(f'{"obs used":>63}', after=1)

        # Write source stats
        total_scheduled = total_correlated = total_used = 0
        for name, src in self.schedule.sources.items():
            if (scheduled := src['scheduled_obs']) == 0:
                continue
            total_scheduled += scheduled
            stats = self.vgosdb.stats.get(name, {'corr': 0, 'used': 0})
            total_correlated += stats['corr']
            total_used += stats['used']
            percent = stats['used'] / scheduled * 100
            self.write(f'  {name:<8s} {scheduled:14d} {stats["corr"]:11d} {stats["used"]:8d} {percent:14.1f}%')
        self.write('  ------------  ---------  ----------   ------          ------')
        percent = total_used / total_scheduled * 100
        self.write(f'  Source Total  {total_scheduled:9d} {total_correlated:11d} {total_used:8d} {percent:14.1f}%')
        # Write footer
        self.write(' * Correlated: included in database', before=1, after=1)

    def write_baseline_performance(self):
        if not self.schedule:
            return
        # select source of statistics
        bl_stats, sep = (self.spool.runs[0].stats['baselines'], '|') if self.spool else (self.vgosdb.stats, '-')
        # Write header
        self.write('-' * 41, after=1)
        self.write('Baseline Performance', after=1)
        self.write(f'{"Number of Observations":>55s}', after=1)
        self.write(f'{"Scheduled   Recoverable*         Used   % of scheduled":>79s}')
        self.write(f'{"obs used":>76}', after=1)

        # Write baseline stats
        names = sorted(self.schedule.stations['names'].keys())
        total_scheduled = total_recoverable = total_used = 0
        for index, fr in enumerate(names):
            for to in names[index+1:]:
                name = f'{fr}-{to}'
                scheduled = self.schedule.baselines[name]
                total_scheduled += scheduled
                if scheduled == 0:
                    self.write('  {0:<17s} {1:14d} {2:>13s} {2:>13s} {3:11.1f}%'.format(name, scheduled,
                                                                                        'NO DATA', 0.0))
                elif name not in self.vgosdb.stats:
                    reason = 'MISSED' if fr in self.schedule.missed or to in self.schedule.missed else 'NOT CORR'
                    self.write(f'  {name:<17s} {scheduled:14d} {reason:>13s} {reason:>13s} {0.0:11.1f}%')
                else:  # Baseline names are separated by |
                    stats = bl_stats.get(f'{fr}{sep}{to}', bl_stats.get(f'{to}{sep}{fr}', {'recov': 0, 'used': 0}))
                    total_recoverable += stats['recov']
                    total_used += stats['used']
                    used = 'DESELECTED' if name in self.vgosdb.deselected_bl else str(stats['used'])
                    percent = stats['used'] / scheduled * 100
                    self.write(f'  {name:<17s} {scheduled:14d} {stats["recov"]:13d} {used:>13s} {percent:11.1f}%')

        self.write('  -----------------      ---------   -----------    ----------       ------')
        percent = self.vgosdb.used / self.schedule.scheduled_obs * 100
        self.write(f'  Baseline Total   {total_scheduled:15d} {total_recoverable:13d} '
                   f'{total_used:13d} {percent:11.1f}%')

        # Write footer
        self.write('  * Recoverable: can be included in the solution.', before=1, after=1)
        self.write_lines('  MISSED:    ', ['Baseline was scheduled, but at least one of the sites', 'did not observe.'])
        self.write_lines('  NOT CORR:  ', ['Baseline was scheduled but not correlated.'], after=0)
        self.write_lines('  NO DATA:   ', ['The baseline\'s stations were both correlated,',
                                           'but they did not observe together.'])
        self.write_lines('  DESELECTED:', ['Usable data was generated for the baseline,',
                                           'but the analyst rejected it.'], after=1)


# Class use to update GLO_ARC_FILE
class INTreport(STDreport):

    def __init__(self, session, vgosdb, spool):
        super().__init__(session, vgosdb, spool)
        self.correlated_sources = defaultdict(lambda: 0)

    def is_corr(self, start, stop, time_list):
        for t in time_list:
            if start <= t <= stop:
                return True
        return False

    def get_correlated_data(self):
        if not self.schedule:
            return []

        # Make a dictionary of scans using baseline-source as Key
        corr = defaultdict(list)
        for index, bl, src, utc, qc_x, qc_s, fc_x, fc_s, flg in self.vgosdb.get_all_obs():
            # Store time of each scan
            key = f'{bl[0]}:{bl[1]}:{src}'
            corr[key].append(utc)
            # Store number of correlated scans for each source
            self.correlated_sources[src] += 1

        removed = set(self.schedule.missed) | set(self.vgosdb.deselected_st)
        not_corr = []
        # Extract list of uncorrelated scans.
        for index, obs in enumerate(self.schedule.obs_list):
            fr = obs['fr']
            fr_name = self.schedule.stations['codes'][fr]['name']
            if fr_name not in self.vgosdb.station_list or fr_name in removed:
                continue
            to = obs['to']
            to_name = self.schedule.stations['codes'][to]['name']
            if to_name not in self.vgosdb.station_list or to_name in removed:
                continue

            scan = obs['scan']
            duration = min(scan['station_codes'][fr]['duration'], scan['station_codes'][to]['duration'])
            start = scan['start']
            stop = start + timedelta(seconds=duration)
            if (key := f'{fr_name}:{to_name}:{obs["scan"]["source"]}') not in corr:
                key = f'{to_name}:{fr_name}:{obs["scan"]["source"]}'
            if key not in corr or not self.is_corr(start, stop, corr[key]):
                not_corr.append(f'{obs["scan"]["source"]:8s} at {start.strftime("%H:%M:%S")}')
        return not_corr

    def write_comments(self, key, comments, in_line=None):
        # Write analysts comments
        self.write_lines(Comments[key], comments, write_empty=(key != 1), after=1)
        if key == 0:
            self.write_missed_obs(in_line)

    def write_missed_obs(self, in_line):
        # Write rejected obs at correlation
        not_corr = self.get_correlated_data()
        unknown = '?'*len(str(self.vgosdb.correlated))
        not_corr_str = str(len(not_corr)) if self.schedule else unknown
        self.write(f'Number of observations rejected during correlation: {not_corr_str:>4s}', after=1)
        if in_line and in_line['correlation']:
            self.write_lines('', in_line['correlation'], write_empty=False, after=1)
        for obs in not_corr:
            self.write(f'    observation of {obs}')
        if not_corr:
            self.write('')

        stats = self.spool.runs[0].stats['session']
        recovered = self.get_total_recoverable()

        unusable = self.vgosdb.correlated - recovered
        excluded = recovered - stats['used']

        # Write not usable observations
        self.write(f'Number of observations not usable by the software: {unusable:5d}', after=1)
        if in_line and in_line['software']:
            self.write_lines('', in_line['software'], write_empty=False, after=1)
        if lst := self.spool.unused.get('unusable', []):
            for obs in lst:
                self.write(obs)
            self.write('')
        elif unusable > 0:
            self.write('    List of unusable observations not available', after=1)

        # Write obs rejected at analysis
        self.write(f'Number of observations rejected during analysis: {excluded:7d}', after=1)
        if in_line and in_line['analysis']:
            self.write_lines('', in_line['analysis'], write_empty=False, after=1)
        if lst := self.spool.unused.get('excluded', []):
            for obs in lst:
                self.write(obs)
            self.write('')
        elif excluded > 0:
            self.write('    List of excluded observations not available', after=1)

    def write_stats(self):
        run = self.spool.runs[0]
        # Check if dUT1 estimation in file.
        if not hasattr(run, 'UEOP'):
            raise Exception('No dUT1 estimation found in the spool file')
        stats = run.stats['session']
        recovered = self.get_total_recoverable()
        sta_clk, sta_atm, clk_brk, sta_coord, bl_clk = defaultdict(list), [], defaultdict(list), defaultdict(list), []
        for info in run.parameters:
            if 'fr' in info.groupdict():
                bl_clk.append(f'{info["fr"].strip()} {info["to"].strip()}')
            else:
                code = info['code']
                sta = info['sta'].strip()
                if code == 'AT':
                    sta_atm.append(sta)
                elif code == 'CL':
                    if (index := info['id']) not in sta_clk[sta]:
                        sta_clk[sta].append(index)
                    elif sta not in clk_brk:
                        clk_brk[sta].append(f'{info["time"].split()[-1]} UT at {sta}')
                elif code == 'BR' and info['id'] == '0':
                    clk_brk[sta].append(f'{info["time"].split()[-1]} UT at {sta}')
                elif code in 'XYZ':
                    sta_coord[sta].append(code)

        nbr_parameters = len(run.parameters) + 1  # UT1
        scheduled = str(self.schedule.scheduled_obs) if self.schedule else '?'*len(str(self.vgosdb.correlated))

        # Write parameterization
        self.write('Parameterization:')
        self.write(f'{nbr_parameters:5d} parameters:')
        self.write('     UT1 offset')
        self.write('     atmosphere offset for:')
        self.write(f'         {" ".join(sta_atm)}')

        self.write('     clock offset, first order term, and second order term for:')
        self.write(f'         {" ".join(list(sta_clk.keys()))}')
        if bl_clk:
            self.write('     baseline clock offset for:')
            self.write(f'         {" ".join(bl_clk)}')
        if sta_coord:
            self.write('     station coordinates for:')
            self.write(f'         {" ".join(list(sta_coord.keys()))}')

        breaks = [brk for item in clk_brk.values() for brk in item] if clk_brk else ['None']
        self.write_lines('Clock breaks: ', breaks, before=1, after=1)
        self.write('Session statistics', after=1)
        self.write_lines('  Observations:',
                         [f'{scheduled:>8s} scheduled',
                          f'{self.vgosdb.correlated:8d} correlated (included in the database)',
                          f'{recovered:8d} recoverable (usable in Solve/nuSolve)', f'{stats["used"]:8d} used'])
        self.write(f'  Session fit: {run.WRMS:13.3f} ps')
        self.write(f'  UT1 formal error: {run.UEOP[2]:7.2f} microsec', after=1)

    def write_source_performance(self):
        if not self.schedule:
            return
        # Write header
        self.write('Source Performance', after=1)
        self.write(f'{"Number of Observations":>42s}', after=1)
        self.write(f'{"Scheduled  Correlated*    Used":>46s}', after=1)

        # Write source stats
        total_scheduled = total_correlated = total_used = 0
        stats = self.spool.runs[0].stats['sources']
        for name, src in self.schedule.sources.items():
            if (scheduled := src['scheduled_obs']) == 0:
                continue
            total_scheduled += scheduled
            obs = stats.get(name, {'recov': 0, 'used': 0})
            correlated = self.correlated_sources.get(name, 0)
            total_correlated += correlated
            total_used += obs['used']
            self.write(f'  {name:<8s} {scheduled:14d} {correlated:11d} {obs["used"]:8d}')
        self.write('  ------------  ---------  ----------   ------')
        self.write(f'  Source Total  {total_scheduled:9d} {total_correlated:11d} {total_used:8d}')
        # Write footer
        self.write_lines(' * ', ['Correlated means included in database.',
                                 'Please note that the number of recoverable (usable) observations',
                                 'cannot be broken down by source for Intensive solutions.'], before=1, after=1)

    def write_station_performance(self):
        pass

    def write_baseline_performance(self):
        pass
