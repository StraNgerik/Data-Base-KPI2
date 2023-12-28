class View:
    def show_message(self, message):
        print(message)

    def show_exec_time(self, time):
        print(f"Query executed in {time} seconds.")
    def get_id(self):
        return int(input("Enter Id/Num: "))

    def get_count(self):
        return int(input("Enter Count: "))

    def show_trains(self, trains):
        print("Trains:")
        for train in trains:
            print(f"Num: {train[0]}, Seats amount: {train[1]}")

    def get_train_input(self):
        stsamnt = input("Enter train seats amount: ")
        return stsamnt

    def show_tickets(self, tickets):
        print("Tickets:")
        for ticket in tickets:
            print(f"Id: {ticket[0]}, Price: {ticket[1]}, Transit_id: {ticket[2]}, Pas_full_name: {ticket[3]}")

    def get_ticket_input(self):
        prc = input("Enter ticket price: ")
        trs_id = input("Enter ticket transit_id: ")
        pas_fn = input("Enter passenger full name: ")
        return prc, trs_id, pas_fn

    def show_stations(self, stations):
        print("Stations:")
        for station in stations:
            print(f"Station ID: {station[0]}, Name: {station[1]}")

    def get_station_input(self):
        name = input("Enter station name: ")
        return name

    def show_transits(self, transits):
        print("Transits:")
        for transit in transits:
            print(f"Train Num: {transit[0]}, Station ID: {transit[1]}, Transit ID: {transit[2]} , Date: {transit[3]}")

    def get_transit_input(self):
        st_id = input("Enter station id: ")
        num = input("Enter train num: ")
        date = input("Enter date")
        return num, st_id, date
