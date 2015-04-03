
from kivy.uix.scrollview import ScrollView
from kivy.uix.boxlayout import BoxLayout

class ScrollBox(ScrollView):
    
    def __init__(self, **kwargs):
    
        self.orientation = kwargs.pop('orientation', 'vertical')
        self.hitem = kwargs.get('hitem', 30)
    
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
            #self.layout.height += (w.height + len(self.layout.children)*(self.layout.spacing/2))
            self.layout.height = len(self.layout.children) * self.hitem + self.layout.spacing *(len(self.layout.children)-1) + self.layout.padding[0] * 2
            print self.layout.height
            w.size_hint_y = None
            w.height = self.hitem
        else:
            self.layout.width += (w.width + len(self.layout.children)*(self.layout.spacing/2))
