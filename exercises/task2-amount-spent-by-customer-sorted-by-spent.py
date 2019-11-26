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
sortedByAmountSpend = totalAmountByCustomer.sortBy(lambda x: x[1], ascending=False)
# another way to sort by amount spend (value), in this case, change the spend amount value to be a key
#flipped = totalByCustomer.map(lambda x: (x[1], x[0]))
#sortedByAmountSpend = flipped.sortByKey()

results = sortedByAmountSpend.collect()
for result in results:
    print(result)