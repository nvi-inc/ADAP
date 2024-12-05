import os
from pathlib import Path

from utils import app

class VGOSDBstatus:

    def __init__(self):
        pass

    def check_masters(self):
        for year in range(1979, 2022):
            path = os.path.join(app.VLBIfolders.control, f'master{year%100:02d}.txt')
            print(path, os.path.exists(path))

    def get_vgosdb(self, pattern):
        # Define time limits and pattern to find files
        found = set()
        # Find all vgosdb wrappers that have been updated during that period
        root = app.VLBIfolders.vgosdb
        for year in os.listdir(root):
            if year.isdigit():
                folder = os.path.join(root, year)
                for file in Path(folder).glob(pattern):
                    found.add(file.parts[-2][:9])
        return found

    def get_non_analyzed(self):
        downloaded = self.get_vgosdb('*/Head.nc')
        print('Downloaded', len(downloaded))
        analyzed = self.get_vgosdb('*/*GSF*kall.wrp')
        print('Analyzed', len(analyzed))
        old = self.get_vgosdb('*/*V001*iIVS*wrp') | self.get_vgosdb('*/*iIVS_kngs.wrp')
        print('Correlated', len(old))
        downloaded -= old
        analyzed -= old
        for ses_id in (downloaded - analyzed):
            print('Non-analyzed', ses_id)
        for ses_id in (analyzed - downloaded):
            print('No head', ses_id)
        test = self.get_vgosdb('*/*iIVS_kngs.wrp')
        print(len(test), len((downloaded - analyzed) - test))

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser( description='Generate NVI weekly or monthly report' )
    parser.add_argument('-c', '--config', help='config file', required=True)

    args = app.init(parser.parse_args())

    status = VGOSDBstatus()
    status.get_non_analyzed()
    #status.check_masters()
