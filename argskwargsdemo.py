


def multiply(num1,num2):
    return num1 * num2


print(multiply(12,2))

def multiplyNumbers(*numbers):
    product =1
    for num in numbers:
        product=product*num
    return product
print(multiplyNumbers(1,2,3))
print(multiplyNumbers(1,2,3,6,7))
#TypeError: multiply() takes 2 positional arguments but 5 were give
#print(multiply(1,2,3,6,7))


def makeSentence(**words):
    sentence=''
    for word in words.values():
        sentence=sentence+word
    return sentence

print(makeSentence(a='kasi',b=' reddy'))


print(makeSentence(a='kasi',b=' reddy', c='this is '))