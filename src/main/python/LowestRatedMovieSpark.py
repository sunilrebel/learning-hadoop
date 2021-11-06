from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("/home/cloudera/sunil/pythonmr/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

sc

nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect()

nums = parallelize([1, 2, 3, 4]
sc.textFile("file:///c:/users/frank/gobs-o-text.txt")

hiveCtx = HiveContext(sc)
rows = hiveCtx.sql("SELECT name, age FROM users")

rdd = sc.parallelize([1, 2, 3, 4])
squaredRDD = rdd.map(lambda x: x*x)

rdd.map(lambda x: x*x)

def squareIt(x):
    return x*x
rdd.map(squareIt)


from pyspark.sql.types import IntegerType
hiveCtx.registerFunction("square", lambda x: x*x, IntegerType())
df = hiveCtx.sql("SELECT square('someNumericFiled') FROM tableName)



movieNames = loadMovieNames()

    lines = sc.textFile("hdfs:///sunil/u.data")

    movieRatings = lines.map(parseInput)

    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))

    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalAndCount[1])

    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    results = sortedMovies.take(10)

    for result in results:
        print(movieNames[result[0]], result[1])
