import csv
import os


def sparkMetricsAgregate(infileLocation, outFileLocation, outputFileName):
    try:
        os.mkdir(outFileLocation)
    except:
        print 'Mod dir already exists!'

# remove < and > chars from file name
    for elem in os.listdir(infileLocation):
        if '<' in elem:
            left = elem.replace('<', '')
            right = left.replace('>', '')
            old = os.path.join(infileLocation, elem)
            new = os.path.join(infileLocation, right)
            os.rename(old, new)

    rawInputs = os.listdir(infileLocation)
    inputs = []
    # Filter unwanted files from directory
    for rw in rawInputs:
        if rw.split('.')[-1] != 'csv':
            print 'Non csv extension found! Ignoring'
        else:
            inputs.append(rw)

    applications = []
    for i in inputs:
        inputFile = os.path.join(infileLocation, i)
        outputFile = os.path.join(outFileLocation, i)
        splitList = i.split('.')
        del splitList[0]
        valueName = '-'.join(splitList)
        with open(inputFile, 'rb') as inFile, open(outputFile, 'wb') as outFile:
            r = csv.reader(inFile)
            w = csv.writer(outFile)
            next(r, None)
            w.writerow(['time', valueName])

            for row in r:
                w.writerow(row)

    rawInputs2 = os.listdir(outFileLocation)
    modInputs = []
    for rw2 in rawInputs2:
        if rw2.split('.')[-1] != 'csv':
            print 'Non csv extension found! Ignoring'
        else:
            modInputs.append(rw2)

    mergedData = {}
    allheaders = []
    for f in modInputs:
        nestedDict = {}
        fileLoc = os.path.join(outFileLocation, f)
        with open(fileLoc, 'r') as file_in:
            r = csv.reader(file_in)
            headers = r.next()
            for h in headers:
                allheaders.append(h)
            time = []
            value = []
            for s in r:
                time.append(s[0])
                value.append(s[1])
                nestedDict = dict(zip(time, value))
            mergedData[headers[1]] = nestedDict
        # print mergedData
        # print time
        # print value
    fieldset = set(allheaders)
    print "Merged datastructure created"
    # print mergedData
    '''
    Aggregation and Encoding based on timestamps
    {timestamp:[{metric1:value}, {metric2:value}]}
    '''
    timeList = []
    for t in mergedData:
        for k, v in mergedData[t].iteritems():
            timeList.append(k)

    unique_time = list(set(timeList))
    # print unique_time

    final = {}
    for k, v in mergedData.iteritems():
        for e in unique_time:
            listMet = []
            nestedDict = {}
            try:
                # print v[e]
                # print k
                # print e
                nestedDict[k] = v[e]
                listMet.append(nestedDict)
                if e not in final.keys():
                    final[e] = listMet
                else:
                    # newlist = []
                    currentList = final[e]
                    newlist = currentList + listMet
                    final[e] = newlist
            except:
                pass
            # print "Timestamp not found, skipping"
    print "Final datastructure has been created"
    with open(outputFileName+'.csv', 'w') as outcsv:
        fieldnames = list(fieldset)
        writer = csv.DictWriter(outcsv, fieldnames=fieldnames)

        writer.writeheader()
        # print mergedData.keys()
        # print mergedData
        for k, v in final.iteritems():
            wdict = {}
            wdict['time'] = k
            for elem in v:
                for k, v in elem.iteritems():
                    wdict[k] = v
            writer.writerow(wdict)
    print "Done"


if __name__ == '__main__':
    expDataLocation = '/Users/Gabriel/Desktop/exp-data/'

    elemDataDir = os.listdir(expDataLocation)

    dirlist = []
    for elem in elemDataDir:
        loc = os.path.join(expDataLocation, elem)
        if os.path.isdir(loc):
            dirlist.append(loc)

    for ed in dirlist:
        outFile = ed.split('/')[-1].split('.')[0]
        outFolder = ed + '.modified'
        #outFile =
        sparkMetricsAgregate(ed, outFolder, outFile)
