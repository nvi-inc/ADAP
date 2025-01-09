import os

from utils import app
from aps.process import APSprocess


# Class use to update GLO_ARC_FILE
class SNR(APSprocess):
    # Initialize class with path
    def __init__(self, path, ses_type):
        super().__init__(path, ses_type, 'snr')

        print('IN SNRANAL')

        self.snranal = self.get_app_path('SNR_PROGRAM')

        # Get INPUT_VMF_DIR
        self.data_dir = self.get_opa_directory('VMF_DATA_DIR')
        if not self.data_dir:
            self.add_error('No valid VMF input data file directory was specified in the OPA configuration file.')
        elif not self.data_dir.endswith('/'):
            self.data_dir += '/'

        # Get output dir for TOTAL and DRY
        self.total_output_dir = self.get_opa_directory('VMF_TOTAL_OUTPUT_DIR')
        self.dry_output_dir = self.get_opa_directory('VMF_DRY_OUTPUT_DIR')
        if not self.total_output_dir and not self.dry_output_dir:
            self.add_error('No valid VMF total or dry output directory were specified in the OPA configuration file.')

    def create_vmf_file(self, vmf_type, out_dir, year, wrapper):

        if not out_dir:
            return

        folder = out_dir.split()[0]
        if 'YEAR' in out_dir:
            folder = os.path.join(folder, year)
            if not os.path.exists(folder):
                os.mkdir(folder)  # , 0o770)
                app.chgrp(folder)

        filepath = os.path.join(folder, wrapper[:9]+'.trp')
        cmd = '{app} {wrapper} {out_file} {input_vmf_dir} {apriori}'.format(app=self.vmf_app, wrapper=wrapper
                                                                            , out_file=filepath, input_vmf_dir=self.data_dir
                                                                            , apriori=vmf_type)

        ans = self.exec(cmd)
        if 'Made {}'.format(filepath) not in ans[-1]:
            for line in ans:
                self.add_error(line)
        if not os.path.exists(filepath):
            self.add_error('{} not created'.format(os.path.basename(filepath)))

    def do_it(self, year, wrapper):

        # Create VMF file for TOTAL and DRY
        self.create_vmf_file('TOTAL', self.total_output_dir, year, wrapper)
        self.create_vmf_file('DRY', self.dry_output_dir, year, wrapper)

        return not self.has_errors
