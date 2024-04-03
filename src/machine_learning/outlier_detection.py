

class OutlierDetection:
    def __init__(self, data, model):
        self.data = data
        self.model = model
        self.model.fit(self.data)
        print('Model fitted')

    def detect(self):
        self.data['anomaly'] = self.model.predict(self.data)
        return self.data
    
    
