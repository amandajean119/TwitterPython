# !/usr/bin/env python
# -*- coding: utf-8 -*-

# ABOD FastABOD and LB-ABOD algorithm implementation
# Code by Marin Young
# Created at 2014/4/25

import csv
import sys
import numpy as np
import math
import codecs
import time
import datetime
from operator import itemgetter
import pickle


####################pre-processing####################
def normalization(rowDataList):
    """
    standardize all the attributes
    use Z-Score method
    """
    npDataList = [float(s) for s in rowDataList]
    npDataList = np.array(npDataList)
    means = npDataList.mean()
    stdDev = npDataList.std()  # standard deviation
    normalizedList = []
    for x in npDataList:
        y = (x - means) / stdDev
        normalizedList.append(y)
    return normalizedList


def attributesSelection(data):
    """
    feature selection
    """
    data = np.array(data)
    (row, column) = data.shape
    coord = []
    tmp = []
    i = 0
    for i in range(column - 3):  # Drop [ID, ClusterID, Metoid]
        if i == 1 or i == 2:  # Drop [timestampArrive, nRadioOn(NULL)]
            continue
        tmp = [data[row][i + 3] for row in xrange(len(data))]  # fetch each column of data
        tmp = normalization(tmp)  # normalization
        coord.append(tmp)
    result = np.transpose(coord)  # transpose
    return result


def dictSortByValue(d, reverse=False):
    """
    sort the given dictionary by value
    """
    return sorted(d.iteritems(), key=itemgetter(1), reverse=False)


def euclDistance(A, B):
    """
    calculate the euclidean metric of @A and @B
    A = (x1, x2, ..., xn)  are initialized
    """
    if len(A) != len(B):
        sys.exit('ERROR\teuclDistance\tpoint A and B not in the same space')
    vector_AB = A - B
    tmp = vector_AB ** 2
    tmp = tmp.sum()
    distance = tmp ** 0.5
    return distance


def computeDistMatrix(pointsList):
    """
    compute distance of each pair of points and stored in distTable
    """
    (nrow, ncolumn) = pointsList.shape
    distTable = [[] for i in range(nrow)]  # define a row * row array
    for i in range(nrow):
        for j in range(i + 1):
            if i == j:
                distTable[i].append(0.0)
                continue
            #mold_ij = euclDistance(pointsList[i], pointsList[j])
            mold_ij = np.linalg.norm(pointsList[i] - pointsList[j])
            distTable[i].append(mold_ij)
    return distTable


def angleBAC(A, B, C, AB, AC):  # AB AC mold
    """
    calculate <AB, AC>
    """
    vector_AB = B - A  # vector_AB = (x1, x2, ..., xn)
    vector_AC = C - A
    mul = vector_AB * vector_AC  # mul = (x1y1, x2y2, ..., xnyn)
    dotProduct = mul.sum()  # dotProduct = x1y1 + x2y2 + ... + xnyn
    try:
        cos_AB_AC_ = dotProduct / (AB * AC)  # cos<AB, AC>
    except ZeroDivisionError:
        sys.exit('ERROR\tangleBAC\tdistance can not be zero!')
    if math.fabs(cos_AB_AC_) > 1:
        print 'A\n', A
        print 'B\n', B
        print 'C\n', C
        print 'AB = %f, AC = %f' % (AB, AC)
        print 'AB * AC = ', dotProduct
        print '|AB| * |AC| = ', AB * AC
        sys.exit('ERROR\tangleBAC\tmath domain ERROR, |cos<AB, AC>| <= 1')
    angle = float(math.acos(cos_AB_AC_))  # <AB, AC> = arccos(cos<AB, AC>)
    return angle


####################ABOD algorithm implement####################
def ABOF(pointsList, A, index, distTable):
    """
    calculate the ABOF of A = (x1, x2, ..., xn)
    """
    i = 0
    varList = []
    for i in range(len(pointsList)):
        if i == index:  # ensure A != B
            continue
        B = pointsList[i]
        if index < i:
            AB = distTable[i][index]
        else:  # index > i
            AB = distTable[index][i]

        j = 0
        for j in range(i + 1):
            if j == index or j == i:  # ensure C != A && B != C
                continue
            C = pointsList[j]
            if index < j:
                AC = distTable[j][index]
            else:  # index > j
                AC = distTable[index][j]

            angle_BAC = angleBAC(A, B, C, AB, AC)

            # compute each element of variance list
            try:
                tmp = angle_BAC / float(math.pow(AB * AC, 2))
            except ZeroDivisionError:
                sys.exit('ERROR\tABOF\tfloat division by zero!')
            varList.append(tmp)
    variance = np.var(varList)
    return variance


def ABOD(X, seed_user):
    """
    ABOD algorithm implementation
    """

    distTable = pickle.load(open('clique_expansion/dist_matrix_' + seed_user + '.p', 'rb'))

    scores = []
    i = 0
    for row in X:
        ABOF_A = ABOF(X, row, i, distTable)
        scores.append(ABOF_A)
        i += 1
    return scores


'''
    outlierList = dictSortByValue(DictABOF)
    outlier = []
    j = 0
    for k, v in outlierList:
        if j < topK:
            outlier.append(k)
            j += 1
            '''
