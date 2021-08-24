def run_if(line, cell=None):
    '''Skips execution of the current line/cell if line evaluates to True.'''
    if not eval(line):
        print('%G-Ai:Info: cell SKIPED based on condition')
        return

    get_ipython().ex(cell)

def load_ipython_extension(shell):
    '''Registers the skip magic when the extension loads.'''
    shell.register_magic_function(run_if, 'line_cell')

def unload_ipython_extension(shell):
    '''Unregisters the skip magic when the extension unloads.'''
    del shell.magics_manager.magics['cell']['run_if']

