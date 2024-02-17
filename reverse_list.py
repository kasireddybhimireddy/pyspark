

li = [1,2,3,4,5,6]
reverse = []
#first method
for i in li[::-1]:
    reverse.append(i)
print(reverse)
#using method
li.reverse()
print(li)
li = [1,2,3,4,5,6]
#reversed
reverse_list = list(reversed(li))
print(reverse_list)

