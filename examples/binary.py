nums = [2, 5, 7, 9, 10, 11, 14, 16, 20]
nums.sort()

target = 3


def binary_search(arr: list[int], target: int):
    left = 0
    right = len(arr) - 1
    boundary_index = -1
    while left <= right:
        mid = left + right // 2
        if arr[mid] >= target:
            boundary_index = mid
            right = mid - 1
        else:
            left = mid + 1

    return boundary_index


index = binary_search([2, 5, 7, 9, 10, 11, 14, 16, 20], 5)
