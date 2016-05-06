#!/usr/bin/env python

import sys
import logging
import random
import subprocess
import csv
import re
from __builtin__ import int
from sklearn.feature_extraction import text
from sklearn.feature_extraction.text import TfidfVectorizer
from ArgumentValidator import checkInputArguments

# Logger initialization
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', datefmt='%d-%m-%Y %I:%M:%S')
logger = logging.getLogger('mr-kmeans')

def generateFinalResult(dataInfo, outputfile, clusters):
    result = dict()
    for i in range(clusters):
        result[i] = dict({"Art and design": 0, "Books": 0, "Business": 0, "Category": 0, "Community": 0, "Culture": 0,
                          "Education": 0, "Environment": 0, "Fashion": 0, "Film": 0, "Football": 0, "Global": 0,
                          "Law": 0, "Life and style": 0, "Media": 0, "Money": 0, "Music": 0, "Politics": 0,
                          "Science": 0, "Society": 0, "Sport": 0, "Technology": 0, "Travel": 0, "UK news": 0,
                          "US news": 0, "World news": 0})

    lines = [line.rstrip('\n') for line in open('./centroids/centroidinfo')]
    centroidCounter = 0
    text_file = open(outputfile, "w")
    text_file.write(" ,Art and design,Books,Business,Category,Community,Culture,Education,Environment,Fashion,Film,Football,Global,Law,Life and style,Media,Money,Music,Politics,Science,Society,Sport,Technology,Travel,UK news,US news,World news\n")
    for line in lines:
        sum=0
        centroid = line.split('|')
        for movieID in centroid[1].split('\t'):
            if len(movieID.strip()) > 0:
                result[centroidCounter][dataInfo[movieID]]+=1
                sum+=1
        text_file.write("Cluster"+str(centroidCounter+1))
        for key in result[centroidCounter]:
            text_file.write(","+str(float('%.3f' % round(result[centroidCounter][key]/float(sum), 3))))
        centroidCounter+=1
        text_file.write('\n')
    text_file.close()



def main(argv):
    hadoopHome = inputfile= outputfile= hadoopJar = ''
    counter = 1
    clusters = 25

    #check input arguments
    checkInputArguments(logger,argv,hadoopHome,inputfile,outputfile,counter,clusters)

    # remove stop words from file
    logger.info("Import Stop words")
    my_words=set(['said'])
    my_stop_words = text.ENGLISH_STOP_WORDS.union(my_words)
    newinputfile = inputfile + ".new"

    category = []
    id = []
    dataInfo = dict()
    all = []
    logger.info("Reading Data")
    with open(inputfile) as inputFile:
        data = csv.reader(inputFile, delimiter='\t')
        # skip header
        next(data, None)
        for row in data:

            # clean up title
            words = row[2].split(' ')
            mytext = ""
            for word in words:
                word = re.sub('[^a-zA-Z\t]+', '', word.lower())
                mytext = mytext + " " + word
            # clean up content
            words = row[3].split(' ')
            for word in words:
                word = re.sub('[^a-zA-Z\t]+', '', word.lower())
                mytext = mytext + " " + word
            all.append(mytext)
            id.append(row[1])
            dataInfo[row[1]] = row[4]

    logger.info("Vectorizing data")
    tfidf_vectorizer = TfidfVectorizer(max_features=1000,stop_words=set(my_stop_words))
    data = tfidf_vectorizer.fit_transform(all)

    allData = 1 - data.A

    #print allData
    logger.info("Dump to new file preprocessed data")
    text_file = open(newinputfile, "w")

    dataCounter = 0
    for row in range(allData.shape[0]):
        text_file.write(id[row] + "|")
        for column in range(allData.shape[1]):
            text_file.write("%s\t" % (allData[row][column]))
        text_file.write('\n')
        dataCounter += 1
    text_file.close()

    #create init random centroids
    usedMovies = []
    text_file = open('./centroids/centroidinfo', "w")
    for i in range(clusters):
        # if random centroid is not a previous choosen one
        randomMovie = random.randint(0, int(allData.shape[0]) - 1)
        while randomMovie in usedMovies:
            randomMovie = random.randint(0, int(allData.shape[0]) - 1)
        # save info to file
        for column in range(allData.shape[1]):
            text_file.write("%s\t" % (float(allData[randomMovie][column])))
        text_file.write('|\n')
        logger.info("Setting init cluster with code [%s] from event with id=[%s]",str(i),id[randomMovie])
        usedMovies.append(randomMovie)
    text_file.close()

    # run 'count' times
    for i in range(counter):
        logger.info("Kmeans: MapReduce execution")
        process = subprocess.Popen(
            hadoopJar + " jar ./hadoop-streaming-2.6.3.jar -mapper ./mapper.py -reducer ./reduce.py -input " + newinputfile + " -output clusteringResults"+str(i),
            shell=True, stdout=subprocess.PIPE)
        process.wait()

    generateFinalResult(dataInfo, outputfile,clusters)



if __name__ == "__main__":
    main(sys.argv[1:]);
