from neo4j import GraphDatabase

driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "password"))

def menu():
    print (" ")
    print ("  Neo4j queries ")
    print ("-----------------------------")
    print ("1. Retrieve irectors whose imdb is more than specified ")
    print ("2. Retrieve movies with a larger budget than specified ")
    print ("3. Retrieve all movies by one director ")
    print ("4. Retrieve all screenings of a single film and where they take place ")
    print ("5. Retrieve all one directors film screenings ")
    print ("6. Retrieve where all one directors films are shown ")
    print ("7. Retrieve when and where all one directors films are shown ")
    print ("8. Retrieve shortest distance from the chosen start point to the specified end point ")
    print ("9. Retrieve the length of the shortest distance ")
    print ("10. Retrieve average amount of one directors film budgets ")  
    print ("11. Retrieve average amount of one directors film screnning costs ")
    print ("0. Exit ")
    print (" ")
    return int(input ("Choose your option: "))


def add_director(tx, name, imdb, title, budget):
    tx.run("MERGE (a:Director {name:  $name, imdb:  $imdb}) "
           "MERGE (a)-[:DIRECTED]->(b:Film {title: $title, budget: $budget})",
           name = name, imdb = imdb, title = title, budget = budget)


def add_movie_screening(tx, title, budget, time, cost):
    tx.run("MERGE (b:Film {title:  $title, budget:  $budget}) "
           "MERGE (b)-[:SCREENED]->(c:MovieScreening {time: $time, cost: $cost})",
           time = time, cost = cost, title = title, budget = budget)

def add_cinema(tx, time, cost, name, address):
    tx.run("MERGE (c:MovieScreening {time: $time, cost: $cost}) "
           "MERGE (d:Location:Cinema {name: $name, address: $address})"
           "MERGE (c)-[:LOCATED]->(d)",
           time = time, cost = cost, name = name, address = address)


def add_relationship(tx, name, address, name1, address1, distance):
    tx.run("MERGE (d:Location:Cinema {name: $name, address: $address}) "
           "MERGE (e:Location:Cinema {name: $name1, address: $address1}) "
           "MERGE (d)-[r:DISTANCE]->(e) "
           "SET r.distance = $distance",
          name = name, address = address, name1 = name1, address1 = address1, distance = distance)


def add_location(tx, name, address, name1, address1, distance):
    tx.run("MERGE (d:Location:Cinema {name: $name, address: $address}) "
           "MERGE (e:Location:InfoCenter {name: $name1, address: $address1}) "
           "MERGE (d)-[r:DISTANCE]->(e) "
           "SET r.distance = $distance",
          name = name, address = address, name1 = name1, address1 = address1, distance = distance)

# queries

# 1 ---------------------------------------------------------------------------------------
# prints all directors whose imdb is more than specified
def director_by_imdb(tx, imdb):
    print("Direcors with imdb bigger than " + str(imdb))
    print(" ")
    print("Director: ")
    for record in tx.run("MATCH (a:Director) WHERE a.imdb > $imdb "
                         "RETURN a.name, a.imdb ", imdb = imdb):
        print(record["a.name"], " ", record["a.imdb"])

# prints movies with a larger budget than specified
def film_by_budget(tx, budget):
    print("Films with budget bigger than " + str(budget))
    print(" ")
    print("Film: ")
    for record in tx.run("MATCH (a:Film) WHERE a.budget > $budget "
                         "RETURN a.title, a.budget ", budget = budget):
        print(record["a.title"], " ", record["a.budget"])


# 2 ---------------------------------------------------------------------------------------
# prints all movies by one director
def directors_films(tx, name):
    print("Direcor: " + name)
    print(" ")
    print("Films: ")
    for record in tx.run("MATCH (a:Director)-[:DIRECTED]->(b:Film) WHERE a.name = $name "
                         "RETURN b.title ORDER BY b.title", name = name):
        print(record["b.title"])

# prints all screenings of a single film and where they take place
def screenings_by_film(tx, title):
    print("Film " + title + ":")
    print(" ")
    print("Movie screenings and where are they shown: ")
    for record in tx.run("MATCH (a:Film)-[:SCREENED]->(b:MovieScreening) WHERE a.title = $title "
                         "MATCH (b:MovieScreening)-[:LOCATED]->(c:Cinema) "
                         "RETURN b.time, b.cost, c.address, c.name ORDER BY b.cost", title = title):
        print(record["b.time"], " ", record["b.cost"], " ", record["c.name"], " ", record["c.address"])


# 3 ---------------------------------------------------------------------------------------
# finds all one directors film screenings
def screenings_by_director(tx, name):
    title1 = " "
    title2 = " "
    print("Director: " + name)
    print(" ")
    print("Movie screenings: ")
    for record in tx.run("MATCH (a:Director)-[:DIRECTED]->(b:Film) WHERE a.name = $name "
                         "MATCH (b:Film)-[:SCREENED]->(c:MovieScreening) "
                         "RETURN b.title, c.time, c.cost ORDER BY c.cost", name = name):
        title2 = title1
        title1 = record["b.title"]
        if title1 != title2:
            print(" ")                 
            print(title1)
        print(record["c.time"], " ", record["c.cost"])


# finds where all one directors films are shown
def cinemas_by_director(tx, name):
    title1 = " "
    title2 = " "
    print("Director: " + name)
    print(" ")
    print("Movie screenings: ")
    for record in tx.run("MATCH (a:Director)-[:DIRECTED]->(b:Film) WHERE a.name = $name "
                         "MATCH (b:Film)-[:SCREENED]->(c:MovieScreening) "
                         "MATCH (c:MovieScreening)-[:LOCATED]->(d:Cinema) "
                         "RETURN b.title, d.address, d.name ORDER BY b.title", name = name):
        title2 = title1
        title1 = record["b.title"]
        if title1 != title2: 
            print(" ")                     
            print(title1)
        print(record["d.name"], " ", record["d.address"])


# finds when and where all one directors films are shown
def screenings_and_cinemas_by_director(tx, name):
    title1 = " "
    title2 = " "
    print("Director: " + name)
    print(" ")
    print("Movie screenings: ")
    for record in tx.run("MATCH (a:Director)-[:DIRECTED]->(b:Film) WHERE a.name = $name "
                         "MATCH (b:Film)-[:SCREENED]->(c:MovieScreening) "
                         "MATCH (c:MovieScreening)-[:LOCATED]->(d:Cinema) "
                         "RETURN b.title, c.time, c.cost, d.address, d.name ORDER BY c.cost", name = name):
        title2 = title1
        title1 = record["b.title"]
        if title1 != title2: 
            print(" ")                     
            print(title1)
        print(record["c.time"], " ", record["c.cost"], " ", record["d.name"], " ", record["d.address"])
        print(" ")


# 4 ---------------------------------------------------------------------------------------
# finds shortest distance from the chosen start point to the specified end point
def shortest_distance(tx, name, address):
    print("Shortest path from Info center to " + name + ":")
    print(" ")
    print("Address and distance from start point: ")
    print(" ")
    for record in tx.run("""MATCH (start:Location {name: 'Info center', address: 'Info address'}), (end:Location {name: $name, address: $address})
                CALL gds.alpha.shortestPath.stream({
                nodeProjection: 'Location',
                relationshipProjection: {
                    DISTANCE: {
                        type: 'DISTANCE',
                        properties: 'distance',
                        orientation: 'UNDIRECTED'
                    }
                },
                startNode: start,
                endNode: end,
                relationshipWeightProperty: 'distance'
                })
                YIELD nodeId, cost
                RETURN gds.util.asNode(nodeId).address AS address, cost""", name = name, address = address):
            print(record["address"], record["cost"])


# 5 ---------------------------------------------------------------------------------------
# finds the length of the shortest distance
def length(tx, name, address):
    path_length = 0
    print("Length of the shortest path from Info center to " + name + ":")
    print(" ")
    for record in tx.run("""MATCH (start:Location {name: 'Info center', address: 'Info address'}), (end:Location {name: $name, address: $address})
                CALL gds.alpha.shortestPath.stream({
                nodeProjection: 'Location',
                relationshipProjection: {
                    DISTANCE: {
                        type: 'DISTANCE',
                        properties: 'distance',
                        orientation: 'UNDIRECTED'
                    }
                },
                startNode: start,
                endNode: end,
                relationshipWeightProperty: 'distance'
                })
                YIELD nodeId, cost
                RETURN cost""", name = name, address = address):
            path_length = record["cost"]
    print(path_length)


# average amount of one directors film budgets 
def aggregating_budget(tx, name):
    print("Direcor: " + name)
    print(" ")
    print("Average budget: ")
    for record in tx.run("MATCH (a:Director)-[:DIRECTED]->(b:Film) WHERE a.name = $name "
                         "RETURN avg(b.budget) as average", name = name):
        print(record["average"])


# average amount of one directors film screnning costs
def aggregating_cost(tx, name):
    print("Direcor: " + name)
    print(" ")
    print("Average film screening cost: ")
    for record in tx.run("MATCH (a:Director)-[:DIRECTED]->(b:Film) WHERE a.name = $name "
                         "MATCH (b:Film)-[:SCREENED]->(c:MovieScreening) "
                         "RETURN avg(c.cost) as average", name = name):
        print(record["average"])



with driver.session() as session:
    # directors and their films
    session.write_transaction(add_director, 'Brad Bird', 100, 'Ratatouille', 150)
    session.write_transaction(add_director, 'Brad Bird', 100, 'The Incredibles', 92)
    session.write_transaction(add_director, 'Brad Bird', 100, 'The Incredibles 2', 200) 
    session.write_transaction(add_director, 'Andrew Adamson', 101, 'Shrek', 125)
    session.write_transaction(add_director, 'Ben Sharpsteen', 102, 'Dumbo', 13)
    session.write_transaction(add_director, 'Lee Unkrich', 103, 'Coco', 175)
    session.write_transaction(add_director, 'Lee Unkrich', 103, 'Toy Story', 30)

    # films and their screenings
    session.write_transaction(add_movie_screening, 'Ratatouille', 150, '21:17', 29)
    session.write_transaction(add_movie_screening, 'Ratatouille', 150, '1:53', 22)
    session.write_transaction(add_movie_screening, 'Shrek', 125, '17:00', 12)
    session.write_transaction(add_movie_screening, 'Shrek', 125, '9:30', 6)
    session.write_transaction(add_movie_screening, 'Shrek', 125, '11:10', 16)
    session.write_transaction(add_movie_screening, 'Dumbo', 13, '23:25', 15)
    session.write_transaction(add_movie_screening, 'Coco', 175, '7:00', 9) 
    session.write_transaction(add_movie_screening, 'The Incredibles', 92, '20:50', 11) 
    session.write_transaction(add_movie_screening, 'Toy Story', 30, '13:00', 12) 
    session.write_transaction(add_movie_screening, 'Toy Story', 30, '19:30', 20) 

    # screenings and cinemas
    session.write_transaction(add_cinema, '21:17', 29, 'Cinema 1', 'Address 1')
    session.write_transaction(add_cinema, '9:30', 6, 'Cinema 1', 'Address 1')
    session.write_transaction(add_cinema, '1:53', 22, 'Cinema 6', 'Address 6')
    session.write_transaction(add_cinema, '17:00', 12, 'Cinema 1', 'Address 1')
    session.write_transaction(add_cinema, '11:10', 16, 'Cinema 2', 'Address 2')
    session.write_transaction(add_cinema, '23:25', 15, 'Cinema 3', 'Address 3')
    session.write_transaction(add_cinema, '7:00', 9, 'Cinema 3', 'Address 3') 
    session.write_transaction(add_cinema, '20:50', 11, 'Cinema 4', 'Address 4') 
    session.write_transaction(add_cinema, '13:00', 12, 'Cinema 4', 'Address 4') 
    session.write_transaction(add_cinema, '19:30', 20, 'Cinema 3', 'Address 3') 

    # relationships between locations
    session.write_transaction(add_relationship, 'Cinema 1', 'Address 1', 'Cinema 2', 'Address 2', 3)
    session.write_transaction(add_relationship, 'Cinema 1', 'Address 1', 'Cinema 5', 'Address 5', 4)
    session.write_transaction(add_relationship, 'Cinema 1', 'Address 1', 'Cinema 6', 'Address 6', 8)
    session.write_transaction(add_relationship, 'Cinema 2', 'Address 2', 'Cinema 3', 'Address 3', 12)
    session.write_transaction(add_relationship, 'Cinema 2', 'Address 2', 'Cinema 5', 'Address 5', 15)
    session.write_transaction(add_relationship, 'Cinema 3', 'Address 3', 'Cinema 4', 'Address 4', 7)
    session.write_transaction(add_relationship, 'Cinema 3', 'Address 3', 'Cinema 5', 'Address 5', 9)
    session.write_transaction(add_relationship, 'Cinema 4', 'Address 4', 'Cinema 5', 'Address 5', 18)
    session.write_transaction(add_relationship, 'Cinema 5', 'Address 5', 'Cinema 6', 'Address 6', 3)
    session.write_transaction(add_location, 'Cinema 5', 'Address 5', 'Info center', 'Info address', 13)
    session.write_transaction(add_location, 'Cinema 4', 'Address 4', 'Info center', 'Info address', 6)
    session.write_transaction(add_location, 'Cinema 6', 'Address 6', 'Info center', 'Info address', 10)
    session.write_transaction(add_location, 'Cinema 3', 'Address 3', 'Info center', 'Info address', 9)


    print(" ")
    loop = 1
    choice = 0
    while loop == 1:
        choice = menu()
        print(" ")
        if choice == 1:
            session.read_transaction(director_by_imdb, 101)
        elif choice == 2:
            session.read_transaction(film_by_budget, 130)
        elif choice == 3:
            session.read_transaction(directors_films, 'Brad Bird')
        elif choice == 4:
            session.read_transaction(screenings_by_film, 'Shrek')
        elif choice == 5:
            session.read_transaction(screenings_by_director, 'Brad Bird')
        elif choice == 6:   
            session.read_transaction(cinemas_by_director, 'Brad Bird')
        elif choice == 7:
            session.read_transaction(screenings_and_cinemas_by_director,'Brad Bird')
        elif choice == 8:
            session.read_transaction(shortest_distance, 'Cinema 1', 'Address 1')
        elif choice == 9:
            session.read_transaction(length, 'Cinema 1', 'Address 1')
        elif choice == 10:
            session.read_transaction(aggregating_budget, 'Brad Bird')
        elif choice == 11:
            session.read_transaction(aggregating_cost, 'Brad Bird')
        elif choice == 0:
            loop = 0


driver.close()


  