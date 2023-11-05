from solutionbase import Solution, GeneratedData, as_mongo_dict
from pymongo import MongoClient
from collections import Counter


class RelationalSolution(Solution):
    def __init__(self, connection):
        self.connection = connection

    def populate(self, generated_data: GeneratedData):
        with self.connection:
            with self.connection.cursor() as cursor:
                with open("schema.sql", "r") as f:
                    cursor.execute(f.read())

                for user in generated_data.users:
                    cursor.execute(
                        "INSERT INTO users (id, name, password) VALUES (%s, %s, %s)",
                        (user.id, user.name, user.password)
                    )

                for hobby in generated_data.hobbies:
                    cursor.execute(
                        "INSERT INTO hobbies (id, name) VALUES (%s, %s);",
                        (hobby.id, hobby.name)
                    )

                for resume in generated_data.resumes:
                    cursor.execute(
                        "INSERT INTO resumes (id, user_id, city, company) VALUES (%s, %s, %s, %s) ",
                        (resume.id, resume.user_id, resume.city, resume.company)
                    )

                for resumes_hobbies_entry in generated_data.resumes_hobbies:
                    cursor.execute(
                        "INSERT INTO resumes_hobbies (resume_id, hobby_id) VALUES (%s, %s) ",
                        (resumes_hobbies_entry.resume_id, resumes_hobbies_entry.hobby_id)
                    )

    def get_resume_for_user(self, user_name: str):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select resumes.*
                    from users join resumes on users.id = resumes.user_id
                    where users.name = %s
                    limit 1
                """, (user_name,))
                return cursor.fetchone()

    def get_all_hobbies_from_resumes(self):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select distinct hobbies.name
                    from resumes_hobbies join hobbies on resumes_hobbies.hobby_id = hobbies.id
                    order by hobbies.name
                """)
                return cursor.fetchall()

    def get_all_cities_from_resumes(self):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select distinct city
                    from resumes
                    order by city
                """)
                return [r.city for r in cursor.fetchall()]

    def get_all_hobbies_of_users_from_city(self, city: str):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select distinct hobbies.name
                    from users
                        join resumes on users.id = resumes.user_id
                        join resumes_hobbies on resumes.id = resumes_hobbies.resume_id
                        join hobbies on resumes_hobbies.hobby_id = hobbies.id
                    where resumes.city = %s
                    order by hobbies.name
                """,
                               (city,))

                return [r.name for r in cursor.fetchall()]

    def get_all_users_from_same_company(self):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select name
                    from users join (
                        select distinct r1.user_id as id
                        from resumes r1 join resumes r2 on r1.company = r2.company
                        where r1.user_id <> r2.user_id
                    ) user_ids on users.id = user_ids.id
                    order by name
                """)
                return cursor.fetchall()


class DocumentSolution(Solution):
    def __init__(self, mongo_client: MongoClient):
        self.mongo_client = mongo_client
        self.db = mongo_client['test_database']

    def populate(self, generated_data: GeneratedData):
        self.mongo_client.drop_database('test_database')

        for user in generated_data.users:
            self.db.users.insert_one(as_mongo_dict(user))

        for hobby in generated_data.hobbies:
            self.db.hobbies.insert_one(as_mongo_dict(hobby))

        resume_dicts = [{'hobbies': [], **as_mongo_dict(resume)} for resume in generated_data.resumes]
        for resumes_hobbies_entry in generated_data.resumes_hobbies:
            resume_dicts[resumes_hobbies_entry.resume_id]['hobbies'].append(
                generated_data.hobbies[resumes_hobbies_entry.hobby_id].name
            )

        self.db.resumes.insert_many(resume_dicts)

    def get_resume_for_user(self, user_name: str):
        user_id = self.db['users'].find_one({'name': user_name})['_id']
        return self.db['resumes'].find_one({'user_id': user_id})

    def get_all_hobbies_from_resumes(self):
        return self.db['resumes'].distinct('hobbies')

    def get_all_cities_from_resumes(self):
        return list(self.db['resumes'].distinct('city'))

    def get_all_hobbies_of_users_from_city(self, city: str):
        return list(self.db['resumes'].find({'city': city}).distinct('hobbies'))

    def get_all_users_from_same_company(self):
        companies_counts = Counter(r['company'] for r in self.db['resumes'].find({}, {'company': 1}))

        companies_of_interest = [company for company, count in companies_counts.items() if count > 1]
        user_ids = list(
            r['user_id'] for r in self.db['resumes'].find({'company': {'$in': companies_of_interest}}, {'user_id': 1}))
        return list(r['name'] for r in self.db['users'].find({'_id': {'$in': user_ids}}))


class GraphSolution(Solution):
    def __init__(self, driver):
        self.driver = driver

    def populate(self, generated_data: GeneratedData):
        self.driver.execute_query('MATCH (n) DETACH DELETE n')

        for user in generated_data.users:
            self.driver.execute_query('CREATE (:User {id: $id, name: $name})', id=user.id, name=user.name)

        for resume in generated_data.resumes:
            self.driver.execute_query('MERGE (c:Company {name: $name})', name=resume.company)

            self.driver.execute_query("""
            MATCH (u:User {id: $user_id})
            CREATE (r:Resume {id: $resume_id, city: $city})-[:BELONGS_TO]->(u)
            """, user_id=resume.user_id, resume_id=resume.id, city=resume.city)

            self.driver.execute_query("""
            MATCH (r:Resume {id: $resume_id}), (c:Company {name: $company}) 
            CREATE (r)-[:AT]->(c)
            """, resume_id=resume.id, company=resume.company)

        for hobby in generated_data.hobbies:
            self.driver.execute_query('CREATE (:Hobby {id: $id, name: $name})', id=hobby.id, name=hobby.name)

        for entry in generated_data.resumes_hobbies:
            self.driver.execute_query("""
            MATCH (h:Hobby {id: $hobby_id}), (r:Resume {id: $resume_id})
            CREATE (r)-[:HAS]->(h)
            """, hobby_id=entry.hobby_id, resume_id=entry.resume_id)

    def get_resume_for_user(self, user_name: str):
        return self.driver.execute_query("""
        MATCH (r:Resume)-[:BELONGS_TO]->(u:User)
        WHERE u.name = $user_name
        RETURN r
        """, user_name=user_name).records[0].values()[0]

    def get_all_hobbies_from_resumes(self):
        return self.driver.execute_query('MATCH (r:Resume)-[:HAS]->(h:Hobby) RETURN COLLECT(distinct h.name)').records[
            0].values()[0]

    def get_all_cities_from_resumes(self):
        return self.driver.execute_query('MATCH (r:Resume) RETURN COLLECT(distinct r.city)').records[0].values()[0]

    def get_all_hobbies_of_users_from_city(self, city: str):
        return self.driver.execute_query(
            """
            MATCH (r:Resume)-[:BELONGS_TO]->(u:User),
                  (r:Resume)-[:HAS]->(h:Hobby)
            WHERE r.city = $city
            RETURN COLLECT(distinct h.name)
            """,
            city=city
        ).records[0].values()[0]

    def get_all_users_from_same_company(self):
        return self.driver.execute_query(
            """
            match (u1:User)<-[:BELONGS_TO]-(r1:Resume)-[:AT]->(c:Company)<-[:AT]-(r2:Resume)-[:BELONGS_TO]->(u2:User)
            where u1 <> u2
            return collect(distinct u1.name)
            """,
        ).records[0].values()[0]
