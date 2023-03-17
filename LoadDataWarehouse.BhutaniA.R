#FINAL SUBMISSION


#Importing necessary libraries

library(RMySQL)
library(RSQLite)

#Initializing necessary arguments to pass in dbConnect func.

db_user <- "root"
db_password <- "55199@Khoury123!!"
db_name <- "warehouse"
db_host <- "localhost"
db_port <- 3306

dbconSQL <-  dbConnect(MySQL(), user = db_user, password = db_password,
                       dbname = db_name, host = db_host, port = db_port)

dbExecute(conn=dbconSQL, statement='DROP TABLE IF EXISTS Author_Fact')

#Establishing connection with ArticleDB.sqlite

filePath = getwd()
dbfile = "ArticleDB.sqlite"

#Creating a connection with the authorDB database and storing it in dbcon

dbcon <- dbConnect(RSQLite::SQLite(), paste0(filePath, dbfile))


#Storing "required" info from SQLite database into vectors.

Auth_Id <- dbGetQuery(conn=dbcon, statement='SELECT Author_Id FROM Author')

Auth_Id <- Auth_Id$Author_Id

Lastname <- dbGetQuery(conn=dbcon, statement='SELECT LastName FROM Author')

Lastname <- Lastname$LastName

Initials <- dbGetQuery(conn=dbcon, statement='SELECT Initial FROM Author')

Initials <- Initials$Initial

No_of_Articles <- dbGetQuery(conn=dbcon, statement='SELECT Count(Article_Id) AS Art_Count FROM Author_Collab GROUP BY Author_Id')

No_of_Articles <- No_of_Articles$Art_Count

No_of_Coauthors <- c()


#Iterating over each Author Id and calculating the total number of coauthors across all articles.
for(i in 1:length(Auth_Id)){
  id1 = as.character(Auth_Id[i])
  query <- paste('SELECT Article_Id FROM Author_Collab WHERE Author_Id =', id1, sep =" ")
  Auth_Article_Id <- dbGetQuery(conn = dbcon, statement = query)
  Auth_Article_Id <- Auth_Article_Id$Article_Id
  

  res <- c()
  
  #Retrieve the Author Ids for each of the Article Id retrieved above and append the Ids in "res".   
  
  for(j in 1:length(Auth_Article_Id)){
    id2 = as.character(Auth_Article_Id[j])
    query <- paste('SELECT Author_Id FROM Author_Collab WHERE Article_Id =', id2, sep =" ")
    Co_Author_Id <- dbGetQuery(conn = dbcon, statement = query)
    res <- append(res, Co_Author_Id$Author_Id)
  }
  
  #Removing the duplicates from the res and subtracting one from teh length because 
  #the res contains the main author of the article too other than the co-authors 
  
  No_of_Coauthors <- append(No_of_Coauthors, length(unique(res)) - 1)
}

#Creating a Author_Fact dataframe and storing the data frame to SQL table.

Author_Fact_df <- data.frame(Auth_Id, Lastname, Initials, No_of_Articles, No_of_Coauthors)

dbSendQuery(dbconSQL, "SET GLOBAL local_infile = true")
dbWriteTable(dbconSQL, "Author_Fact", Author_Fact_df, overwrite = F, append = T, row.names=F)


#------------------------------------------------------------------------------------------------------------------


#Retrieving the required information and storing into the Jour_Id, Jour_Name, and Years


Jour_Id <- dbGetQuery(conn=dbcon, statement='SELECT Journal_Id FROM Journal')
Jour_Id <- Jour_Id$Journal_Id

Jour_Name <- dbGetQuery(conn=dbcon, statement='SELECT Journal_Title FROM Journal')
Jour_Name <- Jour_Name$Journal_Title

Years <- dbGetQuery(conn=dbcon, statement='SELECT DISTINCT(Year) FROM Article WHERE NOT (Year = "NA")')
Years <- Years$Year

#Initializing necessary vectors to store the aggregated data.

#NOTE: Here 5 vectors related to the article count per year are created. These vectors were hard-coded since the unique values
#      in the Years column in the Article table are only 5 i.e. 1975, 1976, 1977, 1978, 1979.

Art_Cnt_1975 <- c()
Art_Cnt_1976 <- c()
Art_Cnt_1977 <- c()
Art_Cnt_1978 <- c()
Art_Cnt_1979 <- c()
Art_Cnt_Jan <- c()
Art_Cnt_Feb <- c()
Art_Cnt_Mar <- c()
Art_Cnt_Apr <- c()
Art_Cnt_May <- c()
Art_Cnt_Jun <- c()
Art_Cnt_Jul <- c()
Art_Cnt_Aug <- c()
Art_Cnt_Sep <- c()
Art_Cnt_Oct <- c()
Art_Cnt_Nov <- c()
Art_Cnt_Dec <- c()
Art_Cnt_Q1 <- c()
Art_Cnt_Q2 <- c()
Art_Cnt_Q3 <- c()
Art_Cnt_Q4 <- c()

#Looping over Journal Ids and Years to find Article count per year for each of the Journal Ids.
for(i in 1:length(Jour_Id)){
  for(j in 1:length(Years)){
    query <- paste('SELECT Count(Article_Id) AS No_of_Articles FROM Article WHERE Year =', Years[j], 'AND Journal_Id =', Jour_Id[i], sep =" ")
    Art_Count <- dbGetQuery(conn=dbcon, statement=query)
    
    if(Years[j] == 1975){
      Art_Cnt_1975 <- append(Art_Cnt_1975, Art_Count$No_of_Articles)
    }
    else if(Years[j] == 1976){
      Art_Cnt_1976 <- append(Art_Cnt_1976, Art_Count$No_of_Articles)
    }
    else if(Years[j] == 1977){
      Art_Cnt_1977 <- append(Art_Cnt_1977, Art_Count$No_of_Articles)
    }
    else if(Years[j] == 1978){
      Art_Cnt_1978 <- append(Art_Cnt_1978, Art_Count$No_of_Articles)
    }
    else if(Years[j] == 1979){
      Art_Cnt_1979 <- append(Art_Cnt_1979, Art_Count$No_of_Articles)
    }
    
  }
}

#Looping over Journal Ids to find Article count per month for each of the Journal Ids.

for(i in 1:length(Jour_Id)){
  for (j in 1: 12){
    query <- paste('SELECT Count(Article_Id) AS No_of_Articles FROM Article WHERE Month =', j, 'AND Journal_Id =', Jour_Id[i], sep =" ")
    Art_Count <- dbGetQuery(conn=dbcon, statement=query)
    
    if(j == 1){
      Art_Cnt_Jan <- append(Art_Cnt_Jan, Art_Count$No_of_Articles)
    }
    else if(j == 2){
      Art_Cnt_Feb <- append(Art_Cnt_Feb, Art_Count$No_of_Articles)
    }
    else if(j == 3){
      Art_Cnt_Mar <- append(Art_Cnt_Mar, Art_Count$No_of_Articles)
    }
    else if(j == 4){
      Art_Cnt_Apr <- append(Art_Cnt_Apr, Art_Count$No_of_Articles)
    }
    else if(j == 5){
      Art_Cnt_May <- append(Art_Cnt_May, Art_Count$No_of_Articles)
    }
    else if(j == 6){
      Art_Cnt_Jun <- append(Art_Cnt_Jun, Art_Count$No_of_Articles)
    }
    else if(j == 7){
      Art_Cnt_Jul <- append(Art_Cnt_Jul, Art_Count$No_of_Articles)
    }
    else if(j == 8){
      Art_Cnt_Aug <- append(Art_Cnt_Aug, Art_Count$No_of_Articles)
    }
    else if(j == 9){
      Art_Cnt_Sep <- append(Art_Cnt_Sep, Art_Count$No_of_Articles)
    }
    else if(j == 10){
      Art_Cnt_Oct <- append(Art_Cnt_Oct, Art_Count$No_of_Articles)
    }
    else if(j == 11){
      Art_Cnt_Nov <- append(Art_Cnt_Nov, Art_Count$No_of_Articles)
    }
    else if(j == 12){
      Art_Cnt_Dec <- append(Art_Cnt_Dec, Art_Count$No_of_Articles)
    }
    
    }
}

#Looping over Journal Ids to find Article count per quarter for each of the Journal Ids.

for(i in 1:length(Jour_Id)){
    
    query <- paste('SELECT Count(Article_Id) AS No_of_Articles FROM Article WHERE (Month = 1 OR Month = 2 OR Month = 3)', 'AND Journal_Id =', Jour_Id[i], sep =" ")
    Art_Count <- dbGetQuery(conn=dbcon, statement=query)
    Art_Cnt_Q1 <- append(Art_Cnt_Q1, Art_Count$No_of_Articles)
    
    query <- paste('SELECT Count(Article_Id) AS No_of_Articles FROM Article WHERE (Month = 4 OR Month = 5 OR Month = 6)', 'AND Journal_Id =', Jour_Id[i], sep =" ")
    Art_Count <- dbGetQuery(conn=dbcon, statement=query)
    Art_Cnt_Q2 <- append(Art_Cnt_Q2, Art_Count$No_of_Articles)
    
    query <- paste('SELECT Count(Article_Id) AS No_of_Articles FROM Article WHERE (Month = 7 OR Month = 8 OR Month = 9)', 'AND Journal_Id =', Jour_Id[i], sep =" ")
    Art_Count <- dbGetQuery(conn=dbcon, statement=query)
    Art_Cnt_Q3 <- append(Art_Cnt_Q3, Art_Count$No_of_Articles)
    
    query <- paste('SELECT Count(Article_Id) AS No_of_Articles FROM Article WHERE (Month = 10 OR Month = 11 OR Month = 12)', 'AND Journal_Id =', Jour_Id[i], sep =" ")
    Art_Count <- dbGetQuery(conn=dbcon, statement=query)
    Art_Cnt_Q4 <- append(Art_Cnt_Q4, Art_Count$No_of_Articles)
}

#Creating a Journal Fact data frame

Journal_Fact_df <- data.frame(Jour_Id, Jour_Name, Art_Cnt_1975, Art_Cnt_1976, Art_Cnt_1977, Art_Cnt_1978, Art_Cnt_1979,
                              Art_Cnt_Jan , Art_Cnt_Feb, Art_Cnt_Mar, Art_Cnt_Apr, Art_Cnt_May, Art_Cnt_Jun, Art_Cnt_Jul, 
                              Art_Cnt_Aug, Art_Cnt_Sep, Art_Cnt_Oct, Art_Cnt_Nov, Art_Cnt_Dec, Art_Cnt_Q1,Art_Cnt_Q2, 
                              Art_Cnt_Q3, Art_Cnt_Q4)

#Storing the info in the Journal_Fact dataframe into Journal_Fact MySQL table.

dbExecute(conn=dbconSQL, statement='DROP TABLE IF EXISTS Journal_Fact')
dbWriteTable(dbconSQL, "Journal_Fact", Journal_Fact_df, overwrite = F, append = T, row.names=F)





