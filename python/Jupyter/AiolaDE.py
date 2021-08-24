class skipping_class:
    """
    create a structure to map all cells needed activation and the one to skip
    Parameters:
    -----------
        all_op : All options 
        ex_op : the options to execute
        
    """
    def __init__(self,all_op=[],ex_op=[]):
        """
        The initial phase based on param
        create variable for each option and replace space with underscore _
        """
        if type(curr_f) == str:
            curr_f = [curr_f]
        [setattr(self,x.replace(' ','_'),x in curr_f) for x in f_types]
