import ConfigParser
import os
class ibrm_shell():
    def __init__(self):
        self.cfg = self.get_cfg()

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_name = os.path.join('config','config.cfg')
        cfg.read(cfg_name)
        return cfg
    def main(self):
        print self.cfg.get('common','shell_path')

if __name__=='__main__':
    ibrm_shell().main()
