from typing import NamedTuple
import random


def as_mongo_dict(t: NamedTuple):
    result = t._asdict()
    result['_id'] = result.pop('id')
    return result


class User(NamedTuple):
    id: int
    name: str
    password: str


class Hobby(NamedTuple):
    id: int
    name: str


class Resume(NamedTuple):
    id: int
    user_id: int
    city: str
    company: str


class ResumesHobbies(NamedTuple):
    resume_id: int
    hobby_id: int


class GeneratedData(NamedTuple):
    users: [User]
    resumes: [Resume]
    hobbies: [Hobby]
    resumes_hobbies: [ResumesHobbies]


def generate(size: int) -> GeneratedData:
    random.seed(42)

    city_names = [f"City_{i}" for i in range(size)]
    user_names = [f"User_{i}" for i in range(size)]
    hobby_names = [f"Hobby_{i}" for i in range(size)]
    company_names = [f"Company_{i}" for i in range(size)]

    users = [User(id=i, name=name, password='password') for i, name in enumerate(user_names)]
    hobbies = [Hobby(id=i, name=name) for i, name in enumerate(hobby_names)]

    resumes = [Resume(id=user.id, user_id=user.id, city=random.choice(city_names), company=random.choice(company_names))
               for user in users]

    resumes_hobbies: list[ResumesHobbies] = []
    existing_resumes_hobbies_ids: set[tuple[int, int]] = set()

    for _ in range(len(resumes) * 4):
        hobby = random.choice(hobbies)
        resume = random.choice(resumes)
        if (resume.id, hobby.id) in existing_resumes_hobbies_ids:
            continue
        existing_resumes_hobbies_ids.add((resume.id, hobby.id))
        resumes_hobbies.append(ResumesHobbies(resume_id=resume.id, hobby_id=hobby.id))

    return GeneratedData(
        users=users,
        resumes=resumes,
        hobbies=hobbies,
        resumes_hobbies=resumes_hobbies,
    )


class Solution:
    def populate(self, generated_data: GeneratedData):
        pass

    def get_resume_for_user(self, user_name: str):
        pass

    def get_all_hobbies_from_resumes(self):
        pass

    def get_all_cities_from_resumes(self):
        pass

    def get_all_hobbies_of_users_from_city(self, city: str):
        pass

    def get_all_users_from_same_company(self):
        pass


def main():
    print(generate(size=5))


if __name__ == '__main__':
    main()
