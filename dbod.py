import numpy as np
from scipy.spatial.distance import cosine

class DBOD:

    def __init__(self):
        self.normal_user = None

    def fit(self, X):
        normal_user = []
        for column in X.T:
            normal_user.append(np.median(column))
        self.normal_user = np.array(normal_user)

    def decision_function_distance(self, X):
        dists = []
        for row in X:
            dists.append(np.linalg.norm(self.normal_user - row))
        d_max = max(dists)
        # invert so lower values indicate outliers, to match other methods
        return [d_max - d for d in dists]

    def decision_function_angle(self, X):
        dists = []
        for row in X:
            dists.append(cosine(self.normal_user, row))
        return dists
