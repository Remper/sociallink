# Social Media Toolkit
Home to our alignments website, SMT API and UI

## SMT API

### Alignments

| Method                         | GET params                                 | Example |
| ------------------------------ | ------------------------------------------ | ------- |
| alignments/by_twitter_username | `username` – Twitter username, `whitelist` | [example](https://api.futuro.media/smt/alignments/by_twitter_username?username=spacex)  |
| alignments/by_twiiter_id       | `id` – Twitter ID, `whitelist`             | [example](https://api.futuro.media/smt/alignments/by_twitter_id?id=34743251) |
| alignments/by_resource_uri     | `uri` – KB URI                             | [example](https://api.futuro.media/smt/alignments/by_resource_uri?uri=http://dbpedia.org/resource/SpaceX) |
| alignments/datasets            | —                                          | [example](https://api.futuro.media/smt/alignments/datasets) |

`whitelist` — a single or many datasets from which to pull the alignments. The list of datasets can be acquired via the alignments/datasets method

#### Result example

https://api.futuro.media/smt/alignments/by_twitter_username?username=spacex
```
{
  code: 0,
  message: "ok",
  data: {
    request: 34743251,
    alignment: "http://dbpedia.org/resource/SpaceX",
    candidates: [{
        resourceId: "http://dbpedia.org/resource/Design_(UK_band)",
        score: 0.21096046707691185
      }, {
        resourceId: "http://dbpedia.org/resource/Futures_(band)",
        score: 2.5151917732731808
      }, {
        resourceId: "http://dbpedia.org/resource/Future_(rapper)",
        score: 1.6833161768847007
      }, {
        resourceId: "http://dbpedia.org/resource/Rocket_(band)",
        score: 2.650455665454094
      }, {
        resourceId: "http://dbpedia.org/resource/S._(Archdeacon_of_Lewes)",
        score: 1.9267610864689648
      }, {
        resourceId: "http://dbpedia.org/resource/SpaceX",
        score: 3.236673068590393
      }
    ]
  }
}
```

### Profiles

TODO

### Limits

TODO
