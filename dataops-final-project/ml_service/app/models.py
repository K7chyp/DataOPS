class DummyModel:
    def predict(self, features):
        return 1 if sum(features) > 10 else 0

model = DummyModel()
