def main():
    numbers = ["Matias", "Tomas", "Pablo", "Peter"]
    user = numbers.index("Tomas")
    numbers.remove("Tomas")

    print(numbers)


if __name__ == "__main__":
    main()
