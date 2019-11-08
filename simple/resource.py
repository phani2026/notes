available_slots = [[1, 1, 30, 0],
                   [2, 12, 70, 2],
                   [3, 13, 10, 0],
                   [4, 13, 20, 0],
                   [5, 1, 30, 0],
                   [6, 1, 30, 2],
                   [7, 1, 30, 0]
                   ]

required_resource = [[1, 8, 40, 0],
                     [2, 3, 20, 2],
                     [3, 5, 10, 0]
                     ]

# for all slots


i = 1

for i in range(1, 4):

    for j in range(available_slots.__len__() - 1):

        if available_slots[j][i] < available_slots[j + 1][i]:
            temp = available_slots[j]
            available_slots[j] = available_slots[j + 1]
            available_slots[j + 1] = temp


print(available_slots)
