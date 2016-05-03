class Centroid(object):
    movieIds = []
    contextCoords = []
    currentData = [[]]

    def __init__(self):
        self.movieIds = []
        self.contextCoords = []
        self.currentData = []

    def addMovieId(self, id):
        self.movieIds.append(id)

    def addCurrentData(self, event):
        current = []
        for x in event.split('\t'):
            if len(x.strip()) <= 0:
                x = 1.0
            current.append(float(x))
        self.currentData.append(current)

    def getCurrentMovieIds(self):
        return self.movieIds

    def getCurrentCurrentData(self):
        return self.currentData

    def getCurrentCoords(self):
        return self.contextCoords

    def cleanMovies(self):
        self.movieIds = []

    def cleanCurrentCata(self):
        self.currentData = [[]]

    def setData(self, row):
        data = row.split('|')
        for x in data[0].split('\t'):
            if len(x.strip()) <= 0:
                x = 1.0
            self.contextCoords.append(float(x))
        self.movieIds = data[1].split('\t')

    def calculateMean(self):
        self.contextCoords = []
        if len(self.currentData) > 0:
            for j in range(len(self.currentData[0])):
                sum = 0
                for i in range(len(self.currentData)):
                    sum += self.currentData[i][j]
                self.contextCoords.append(sum / len(self.currentData))

    def exportCentroid(self):
        event = ""
        for column in self.contextCoords:
            event = event + str(column) + "\t"
        event = event[:-1] + "|"
        for id in self.movieIds:
            event = event + str(id) + "\t"
        event = event + "\n"
        return event
