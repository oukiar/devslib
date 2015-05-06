
from kivy.uix.scrollview import ScrollView
from kivy.uix.boxlayout import BoxLayout
from kivy.properties import StringProperty, NumericProperty

class ScrollBox(ScrollView):
    orientation = StringProperty("default")
    spacing = NumericProperty(10)
    padding = NumericProperty(10)
    
    def __init__(self, **kwargs):
        
        super(ScrollBox, self).__init__(size_hint=(1,1), **kwargs)
        
        
        self.layout = BoxLayout()   
        self.layout.height = 0 
        self.layout.width = 0
        super(ScrollBox, self).add_widget(self.layout)
        
        
    def on_orientation(self, w, val):
        self.layout.orientation = val
        if val == 'vertical':
            self.layout.size_hint_y = None
            self.layout.width = self.width
        else:
            self.layout.size_hint_x = None
            self.layout.height = self.height
            
        self.update_layout_size()
        
    def on_spacing(self, w, val):
        self.layout.spacing = val
        
    def on_padding(self, w, val):
        self.layout.padding = val
        
    def add_widget(self, w, index=0):
        
        if self.orientation == 'vertical':
            
            if len(self.layout.children) == 0:
                self.layout.height = self.layout.padding[0]
                
            self.layout.height += w.height + self.layout.spacing
            
        
        else:
            if len(self.layout.children) == 0:
                self.layout.width = self.layout.padding[0]
                
            self.layout.width += w.width + self.layout.spacing
        
        self.layout.add_widget(w, index)
        
    def update_layout_size(self):
        if self.layout.orientation == 'vertical':
            self.layout.width = self.width
            self.layout.height = self.layout.padding[0]
            self.layout.size_hint_y = None
        else:
            self.layout.height = self.height
            self.layout.width = self.layout.padding[0]
            self.layout.size_hint_x = None
        
        for i in self.layout.children:
            if self.layout.orientation == 'vertical':
                self.layout.height += i.height + self.layout.spacing
                
            else:
                self.layout.width += i.width + self.layout.spacing
                
    def clear(self):
        self.layout.clear_widgets()
        self.layout.width = 0
        self.layout.height = 0
