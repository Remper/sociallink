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
    type: "http://dbpedia.org/ontology/Organisation",
    candidates: [{
      resourceId: "http://dbpedia.org/resource/Future_(rapper)",
      type: "http://dbpedia.org/ontology/Person",
      score: 1.6833161768847007
    },
    {
      resourceId: "http://dbpedia.org/resource/Rocket_(band)",
      type: "http://dbpedia.org/ontology/Organisation",
      score: 2.650455665454094
    },
    {
      resourceId: "http://dbpedia.org/resource/SpaceX",
      type: "http://dbpedia.org/ontology/Organisation",
      score: 3.236673068590393
    },
    {
      resourceId: "http://dbpedia.org/resource/Space_(English_band)",
      type: "http://dbpedia.org/ontology/Organisation",
      score: 0.1442977292954537
    },
    {
      resourceId: "http://dbpedia.org/resource/Space_(French_band)",
      type: "http://dbpedia.org/ontology/Organisation",
      score: 3.163512333075912
    },
    {
      resourceId: "http://dbpedia.org/resource/Space_(TV_channel)",
      type: "http://dbpedia.org/ontology/Organisation",
      score: 3.1678481553542235
    },
    {
      resourceId: "http://dbpedia.org/resource/The_Launch",
      score: 2.2920326658142876
    },
    {
      resourceId: "http://dbpedia.org/resource/The_Rockets_(band)",
      type: "http://dbpedia.org/ontology/Organisation",
      score: 2.294920430809601
    },
    {
      resourceId: "http://dbpedia.org/resource/X_(American_band)",
      type: "http://dbpedia.org/ontology/Organisation",
      score: 1.4759126893085854
    },
    {
      resourceId: "http://dbpedia.org/resource/X_(Australian_band)",
      type: "http://dbpedia.org/ontology/Organisation",
      score: 1.4759126892645242
    }]
  }
}
```

### Profiles

TODO

### Limits

TODO
