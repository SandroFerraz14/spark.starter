from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountSpentByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    amountSpent = float(fields[2])
    return (customerId, amountSpent)

lines = sc.textFile("customer-orders.csv")
totalAmountByCustomer = lines.map(parseLine).reduceByKey(lambda x, y: round(x + y, 2))

results = totalAmountByCustomer.collect()
for result in results:
    print(result)