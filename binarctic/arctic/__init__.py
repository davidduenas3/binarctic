#

from arctic import arctic 

class Arctic(arctic.Arctic):
    HOST='mongodb://arctic:arctic@localhost/arctic'
    def __init__(self,host=None,**kwargs):
        host=host or self.HOST
        super().__init__(host,**kwargs)



    