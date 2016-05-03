#!/usr/bin/env python

import sys
from centroid import Centroid
from sklearn.metrics.pairwise import euclidean_distances

#distance between components method
def calculateDistance(event, centroid):
    return euclidean_distances(event, centroid)


#import centroids from file to check distance for every event
def readCentroids():
    centroids = []
    lines = [line.rstrip('\n') for line in open('./centroids/centroidinfo')]
    for line in lines:
        centroid = Centroid()
        centroid.setData(line)
        centroids.append(centroid)
    return centroids


def main():
    centroids = readCentroids()

    #for every point
    for line in sys.stdin:
        event = line.split('|')
        eventCoords = []
        #clear point from various garbages
        for x in event[1].split('\t'):
            if len(x.strip()) <= 0:
                x = 1.0
            eventCoords.append(float(x))
        #calculate distance with the first centroid
        minDist = calculateDistance(eventCoords, centroids[0].getCurrentCoords())
        minCenter = 0
        #calculate and check the distance for the remaining centroids
        for i in range(1, len(centroids)):
            dist = calculateDistance(eventCoords, centroids[i].getCurrentCoords())
            if dist < minDist:
                minCenter = i
                minDist = dist
        print('%s,%s' % (minCenter, line[:-1]))


if __name__ == "__main__":
    main()
