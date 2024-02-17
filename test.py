rankings = "9, 3, 7, 6, 4"

list = rankings.split(",")
sum = 0.0
for i in list:
    sum = int(i)+ sum
avg = sum/len(list)
print(avg)
