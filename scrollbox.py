
from kivy.uix.scrollview import ScrollView
from kivy.uix.boxlayout import BoxLayout

class ScrollBox(ScrollView):
    
    def __init__(self, **kwargs):
    
        self.orientation = kwargs.pop('orientation', 'vertical')
    
        super(ScrollBox, self).__init__(**kwargs)
            
        if self.orientation == 'vertical':
            self.layout = BoxLayout(orientation='vertical', size_hint_y=None, spacing=10)
            self.layout.height = 0
        else:
            self.layout = BoxLayout(size_hint_x=None, spacing=10)
            self.layout.width = 0
    
    
        super(ScrollBox, self).add_widget(self.layout)
        
    def add_widget(self, w, index=0):
        
        self.layout.add_widget(w)
        
        if self.orientation == 'vertical':
            self.layout.height += (w.height + len(self.layout.children)*(self.layout.spacing/2))
        else:
            self.layout.width += (w.width + len(self.layout.children)*(self.layout.spacing/2))
