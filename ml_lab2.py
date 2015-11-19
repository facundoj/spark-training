from pyspark import SparkContext

import re

sc = SparkContext('local', 'Lab2')

# wordsList = ['cat', 'elephant', 'rat', 'rat', 'cat']
# wordsListRDD = sc.parallelize(wordsList, 4)

# <= [11]
# wordsPluralRDD = wordsListRDD.map(lambda word: word + 's')
# pluralLengthsRDD = wordsPluralRDD.map(lambda word: len(word))
#
# pluralLengths = pluralLengthsRDD.collect();
# print(pluralLengths)

# # (1f)
# # Tuples ('word', 1)
# wordPairs = wordsListRDD.map(lambda word: (word, 1))


# # (2b)
# # Tuples ('word', n)
# counts = wordPairs \
#         .groupByKey() \
#         .map(lambda (word, occurrences): (word, sum(occurrences)))
#
# print(counts.collect())

# # (2c)
# # Tuples ('word', n)
# countsRDD = wordPairs.reduceByKey(lambda count1, count2: count1 + count2)
# print(countsRDD.collect())

# # (2d)
# counts = wordsListRDD \
#     .map(lambda word: (word, 1)) \
#     .reduceByKey(lambda x, y: x + y) \
#     .collect()
#
# print(counts)

# # (3a)
# uniqueWordsCount = wordsListRDD \
#     .map(lambda word: (word, 1)) \
#     .groupByKey() \
#     .count()
#
# print(uniqueWordsCount)

# # (3b) - wordsCount / uniqueWordsCount
# # Tuple ('word', 1)
# wordPairsRDD = wordsListRDD.map(lambda word: (word, 1))
#
# wordsCount = wordsListRDD.count()
#
# uniqueWordsCount = wordsListRDD \
#     .map(lambda word: (word, 1)) \
#     .groupByKey() \
#     .count()
#
# print(float(wordsCount) / uniqueWordsCount)

# ************************* Part 4 *************************
# (4a)
def countWords(wordsRDD):
    return wordsRDD \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x, y: x + y)


def removePunctuation(text):
    return re.sub(r'[^\w\s]|_', '', text) \
        .lower() \
        .strip()


# (4c)
fileRDD = sc.textFile('/home/facundo.jauregui/PycharmProjects/SparkTest/resources/shakespeare.txt')
cleanedFileRDD = fileRDD.map(removePunctuation)

# (4d) & (4e)
wordsRDD = cleanedFileRDD \
    .flatMap(lambda line: line.split(' ')) \
    .filter(lambda word: word != '')

# (4f)
top15Words = countWords(wordsRDD) \
    .takeOrdered(15, lambda (word, count): -count)

print '\n'.join(map(lambda t: '%s:\t%d' % t, top15Words))
