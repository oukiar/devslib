
from kivy.uix.scrollview import ScrollView
from kivy.uix.boxlayout import BoxLayout
from kivy.properties import StringProperty

class ScrollBox(ScrollView):
    
    orientation = StringProperty()
    
    def __init__(self, **kwargs):

        orientation = kwargs.pop("orientation", "vertical")
        
        super(ScrollBox, self).__init__(**kwargs)
        
        if orientation == 'vertical':
            self.layout = BoxLayout(orientation='vertical', size_hint_y=None, spacing=10, padding=10)
            self.layout.height = 0
        else:
            self.layout = BoxLayout(size_hint_x=None, spacing=10, padding=10)
            self.layout.width = 0
    
        super(ScrollBox, self).add_widget(self.layout)
        
    def add_widget(self, w, index=0):
        
        if self.orientation == 'vertical':
            
            if len(self.layout.children) == 0:
                self.layout.height = self.layout.padding[0]
                
            self.layout.height += w.height + self.layout.spacing
            
        
        else:
            if len(self.layout.children) == 0:
                self.layout.width = self.layout.padding[0]
                
            self.layout.width += w.width + self.layout.spacing
        
        print self.layout.size
        print self.orientation
        
        self.layout.add_widget(w, index)
                
    def clear(self):
        self.layout.clear_widgets()
        self.layout.width = 0
        self.layout.height = 0
