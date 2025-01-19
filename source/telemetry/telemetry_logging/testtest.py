def generate_squares(n):
    for i in range(n):
        yield i * i

squares = generate_squares(5)
print(next(squares))  # Output: 0 (first value)
print(next(squares))  # Output: 1 (second value)
print(squares)
# print(next(squares))  # Output: 4 (third value)
# print(next(squares))  # Output: 4 (third value)
# print(next(squares))  # Output: 4 (third value)
