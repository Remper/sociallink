
class MostFollowers:
    """
        Always returns a user with the most followers
    """
    def __init__(self):
        pass

    def predict(self, sample):
        top_followers = -1
        top_candidate = -1
        for i, candidate in enumerate(sample["candidates"]):
            followers_count = candidate["profile"]["followersCount"]
            if followers_count > top_followers:
                top_followers = followers_count
                top_candidate = i
        return top_candidate