class User:
    def __init__(self,user_name,user_id,verified,description):
        self.user_name = user_name
        self.user_id = user_id
        self.verified = verified
        self.description = description


def create_user_object(user):
    return User(user_name=user[1], user_id="@"+user[2], verified=user[3], description=user[5])