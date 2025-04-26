'''
Download 1/1/2015 data from https://www.gharchive.org/

Schema is at https://github.com/igrigorik/gharchive.org/blob/master/bigquery/schema.js 

- Create a new public repository to push your code to.
- Write a function to find top-k users with most overall activity, most number of PRs, most number of issues, and most number of commit pushes
- Use the function to show each of them for k = 10. You would have 4 results here
- Find out if any user pushed a commit right at 12:00pm PST time
- Find how many repositories have between 5 to 10 distinct users pushing commit to it on 1/1/2015.
- Push your code to the repository created at Step 1.

As you implement the above, make sure to write clean code with documentation and type hints. You may use AI.
'''

import os

from datetime import datetime

import json
from typing import Optional, List, Iterable
from dataclasses import dataclass, fields

@dataclass
class Item:
    data: str
    timestamp: int    

@dataclass
class User:
    id: int
    login: str
    url: str
    avatar_url: str

@dataclass
class Repo:
    id: int
    name: str
    url: str

@dataclass
class Action:
    id: str
    type: str   
    public: bool # always true since private actions are not recorded
    actor: User
    repo: Repo
    created_at: str

class DataReader:
    def __init__(self, directory: Optional[str] = None, file: Optional[str] = None) -> None:
        if file:
            self.files: List[str] = [os.path.join(directory, file)]
        elif directory:
            self.files: List[str] = [os.path.join(directory, f) for f in sorted(os.listdir(directory)) if f.endswith('.json')]
        else:
            raise ValueError("Either directory or file must be provided")
        
        self.file_iter: Iterable = iter(self.files)
        self.current_file: Optional[str] = None
        self.current_line_iter: Optional[Iterable] = None

    def _open_next_file(self) -> None:
        try:
            next_file = next(self.file_iter)
            print(f"Opening file: {next_file}")
            self.current_file = open(next_file, 'r', encoding='utf-8', errors='replace')
            self.current_line_iter = iter(self.current_file)
        except StopIteration:
            self.current_file = None
            self.current_line_iter = None

    def _dict_to_obj(self, data, obj_type) -> Action | User | Repo:
        action_keys = {f.name for f in fields(obj_type)}
        filtered_data = {k: v for k, v in data.items() if k in action_keys}
        return obj_type(**filtered_data)

    def __iter__(self) -> Iterable:
        return self

    def __next__(self) -> Action:
        while True:
            if self.current_line_iter == None:
                self._open_next_file()
                if self.current_line_iter == None:
                    raise StopIteration
            try:
                line = next(self.current_line_iter)
                data = json.loads(line)
                action = self._dict_to_obj(data, Action)
                action.actor = self._dict_to_obj(data['actor'], User)
                action.repo = self._dict_to_obj(data['repo'], Repo)
                return action
            except StopIteration:
                self.current_file.close()
                self.current_line_iter = None
            except json.JSONDecodeError:
                print(f"Skipping line: {line}")
            except KeyError as e:
                print(f"Missing key {e} in line: {line}")
            
class AnalyticsEngine:
    def __init__(self, reader: Iterable[Action]) -> None:
        self.actions: List[Action] = list(reader)

    def top_k_users_by(self, target: int, category: Optional[str] = None) -> List[tuple]:
        """
        list of possible categories:
        - None or "activity": any type of actions
        - "prs": PullRequestEvent 
        - "issues": IssuesEvent
        - "commits": PushEvent
        """
        if category == None or category == "activity":
            action_filter = "any"
        elif category == "prs":
            action_filter = "PullRequestEvent"
        elif category == "issues":
            action_filter = "IssuesEvent"
        elif category == "commits":
            action_filter = "PushEvent"
        
        users = {}
        for action in self.actions:
            if action_filter == "any" or action.type == action_filter:
                user = action.actor
                if user.id not in users:
                    users[user.id] = 0
                users[user.id] += 1
        
        sorted_users = sorted(users.items(), key=lambda x: x[1], reverse=True)
        top_k_users = sorted_users[:target]
        return [(user[0], user[1]) for user in top_k_users]
    
    def commits_at(self, target: str) -> bool:
        """
        time should be in 24-hour clock format
        """

        for action in self.actions:
            iso_time = action.created_at
            dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%SZ")
            time_only = dt.strftime("%H:%M:%S")
            if time_only == target:
                return True
        return False
    
    def filter_repo_commit(self, maximum_target: int, minimum_target: Optional[int] = None) -> List[tuple]:
        """
        range = (min, max)
        """
        repos = {}

        for action in self.actions:
            if action.type == "PushEvent":
                repo = action.repo
                user = action.actor
                if repo.id not in repos:
                    repos[repo.id] = {"name": repo.name, "url": repo.url, "users": []}
                if user.id not in repos[repo.id]["users"]:
                    repos[repo.id]["users"].append(user.id)
        
        res = []

        for repo, info in repos.items():
            if (minimum_target == None or len(info["users"]) >= minimum_target) and len(info["users"]) <= maximum_target:
                res.append((info["name"], info["url"]))
        
        return res


jan1_2015 = DataReader(directory="data")
analyise_2015 = AnalyticsEngine(jan1_2015)

target_tops = 10
print(f"Top 10 users on overall activity: {analyise_2015.top_k_users_by(target_tops, category='activity')}")
print(f"Top 10 users on prs: {analyise_2015.top_k_users_by(target_tops, category='prs')}")
print(f"Top 10 users on issues: {analyise_2015.top_k_users_by(target_tops, category='issues')}")
print(f"Top 10 users on commits: {analyise_2015.top_k_users_by(target_tops, category='commits')}")

jan1_2015_1200 = DataReader(directory="data", file="2015-01-01-12.json")
analyise_1200_only = AnalyticsEngine(jan1_2015_1200)

print(f"Action taken right on 12:00:00: {analyise_1200_only.commits_at('12:59:59')}")

print(f"Repos with user count from 5 to 10: {analyise_2015.filter_repo_commit(minimum_target=5, maximum_target=10)}")
