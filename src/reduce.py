#!/usr/bin/env python

import sys
from centroid import Centroid


def main():
    newCentroids = dict()

    # collect all data from mappers
    for line in sys.stdin:

        keyValue = line.split(',')
        dataInfo = keyValue[1].split('|')

        if keyValue[0] not in newCentroids:
            centroid = Centroid()
            centroid.addMovieId(dataInfo[0])
            centroid.addCurrentData(dataInfo[1])
            newCentroids[keyValue[0]]= centroid
        else:
            newCentroids[keyValue[0]].addMovieId(dataInfo[0])
            newCentroids[keyValue[0]].addCurrentData(dataInfo[1])

    #for new centroids calculate means and store them into file
    text_file = open('./centroids/centroidinfo', "w")
    for key in newCentroids:
        print key
        newCentroids[key].calculateMean()
        result = newCentroids[key].exportCentroid()
        text_file.write(result)
    text_file.close()


if __name__ == "__main__":
    main()
