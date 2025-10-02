# context.py
class AnalysisContext:
    def __init__(self, analyzer, menu_commands, show_main_menu_func, 
                 select_detailed_const, select_hiring_const, end_const):
        self.analyzer = analyzer
        self.menu_commands = menu_commands
        self.show_main_menu_func = show_main_menu_func
        self.select_detailed_const = select_detailed_const
        self.select_hiring_const = select_hiring_const
        self.end_const = end_const