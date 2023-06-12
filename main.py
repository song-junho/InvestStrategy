from asset import stock

start_date = "2006-01-01"
end_date   = "2023-06-01"


if __name__ == "__main__":
    #
    stock.Value(start_date, end_date).update_schedule()
    stock.Growth(start_date, end_date).update_schedule()
    stock.Size(start_date, end_date).update_schedule()