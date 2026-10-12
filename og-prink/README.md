# Privacy Preserving Flink

## A fully functional data anonymization solution for Apache Flink
Prink is a data anonymization solution for Apache Flink, that provides k-anonymity and l-diversity for data streams. 
It implements the [CASTLE library](https://ieeexplore.ieee.org/document/5374415) providing strong anonymization guarantees for numerical and non-numerical data, as well as support for multiple sensitive attributes and custom generalizers.

## Getting started
- Install Apache Flink following the official installation [instructions](https://nightlies.apache.org/flink/flink-docs-release-1.15//docs/try-flink/local_installation/).
- Clone repository

## Configurable anonymization profiles (Ganges job)

The job `ganges.GangesEvaluation` (main class of the jar) reads JSON records from Kafka (`processed-topic`),
anonymizes them with Prink and writes JSON to Kafka (`prink-topic`). *What* is anonymized and *how* is defined by
an **anonymization profile** (YAML or JSON), so no recompilation is needed to change the setup.

### Selecting a profile

| Option (first match wins)            | Meaning                                                              |
|--------------------------------------|----------------------------------------------------------------------|
| `--config /path/to/profile.yaml`     | profile file on the machine/container that submits the job (`flink run`) |
| `--profile <name>`                   | profile bundled in the jar (`profiles/<name>.yaml`, see below)       |
| env `PRINK_CONFIG=/path/profile.yaml`| like `--config`                                                      |
| env `PRINK_PROFILE=<name>`           | like `--profile`                                                     |
| *(nothing)*                          | bundled `thesis-default` (the original thesis setup)                 |

The profile is read when the job is submitted; hierarchy files are inlined at that point, so task managers need
no access to the profile or its files.

Command line arguments override the profile: `--k --l --delta --beta --zeta --mu --suppressStrategy --showInfoLoss
--bootstrapServers --inputTopic --outputTopic --groupId`, e.g.

    flink run -m flink-jobmanager:8081 prink-0.6.jar --profile vitals-sensitive --k 10 --delta 250

### Bundled profiles (`og-prink/profiles/`)

| Profile               | Output topic             | Idea                                                                                     |
|-----------------------|--------------------------|------------------------------------------------------------------------------------------|
| `thesis-default`      | `prink-topic`            | original setup: all vitals, NEWS2 sub-scores and ICD-10 generalized + sensitive; k=5, l=3, delta=125; keyed on `userid`, temperature as double |
| `vitals-sensitive`    | `prink-vitals-sensitive` | ward + ICD-10 quasi-identifiers; resp/bps/pulse/spo2/temp sensitive; NEWS2 sub-scores, names dropped; caseid hashed |
| `news2-only`          | `prink-news2-only`       | raw vitals dropped; only generalized NEWS2 sub-scores + partial NEWS2 total released (sensitive), l=2 |
| `qi-demographics`     | `prink-qi-demographics`  | placeholder: age band, sex, ward, ICD-10 as quasi-identifiers once the data contains them |
| `icu-qi-v2`           | `prink-icu-qi-v2`        | ICU record schema v2: ageBand, sex (hierarchies), postcode2, ward, ICD-10 (reduction) as quasi-identifiers; vitals, rhythm (hierarchy), o2 and news2Total sensitive; userid/caseid hashed; k=5, l=2 |

### Profile format

```yaml
name: vitals-sensitive            # used in the Flink job / operator name
privacy:                          # CastleFunction parameters (defaults shown)
  k: 5                            # k-anonymity over distinct data subjects
  l: 3                            # l-diversity over the sensitive fields (0 = off)
  delta: 125                      # tuples held back before the oldest one must be released
  beta: 50000                     # max. non-k-anonymous clusters
  zeta: 10000                     # max. k-anonymous clusters kept for re-use
  mu: 100                         # info-loss values averaged for tau
  suppressStrategy: 2             # 0 = drop, 1 = null generalized fields, 2 = max. generalization
  showInfoLoss: true              # append field "infoloss"
kafka:                            # defaults shown
  bootstrapServers: kafka:29092
  inputTopic: processed-topic
  outputTopic: prink-topic
  groupId: flink-group            # use one group id per job
dataSubjectKey: userid            # Prink counts k over distinct values of this field (= keyBy + posTupleId)
fields:                           # tuple order; fields that are not listed are never read nor written
  - { name: userid,   type: string, role: identifier-passthrough }
  - { name: username, role: drop }
  - name: caseid
    role: mask
    mask: { strategy: hash, saltEnv: PRINK_MASK_SALT, length: 16 }
  - name: icd10
    type: string
    role: quasi-identifier
    generalizer: hierarchy
    hierarchy: { file: hierarchies/icd10_chapter1.yaml }
  - { name: temp, type: double, role: sensitive, generalizer: aggregation, bounds: [30.0, 45.0] }
```

Field attributes:

| Attribute     | Values / meaning |
|---------------|------------------|
| `name`        | JSON key (input and output) |
| `type`        | `int`, `long`, `float`, `double`, `string` (default). Numbers given as text are parsed; missing, JSON `null` and unparsable values become `null` |
| `role`        | `identifier-passthrough`: released unchanged (NoneGeneralizer) · `drop`: removed before anonymization, never written · `mask`: replaced before anonymization by a salted SHA-256 hash (`strategy: hash`, salt via `salt` or – preferred – the env variable named in `saltEnv`, optional `length`) or a constant (`strategy: constant`, `value`) · `quasi-identifier`: generalized, not used for l-diversity · `sensitive`: generalized (if a generalizer is set) and used for l-diversity |
| `generalizer` | `aggregation` (range `(min,max)`, needs `bounds: [min, max]`, numeric types) · `reduction` (trailing characters replaced by `*`, see below) · `hierarchy` (lowest common ancestor in a tree, string type) · `none` |
| `hierarchy`   | `file:` YAML/JSON (`paths:` list) or CSV (one path per line, `;` or `,` separated); looked up as given, relative to the profile, then on the classpath · `paths:` inline list of paths from general to specific · `root:` name of the root (default `<ROOT>`). Without paths the hierarchy is flat (common value or root). |
| `weight`      | optional information-loss weight (CastleRule `infoLossMultiplier`; used if all weights sum to 1) |

There is no boolean type: read booleans as `int` (`true` -> 1, `false` -> 0) and aggregate them with
`bounds: [0, 1]` (`"(0,1)"` = mixed cluster), or as `string` (`"true"`/`"false"`) with a flat hierarchy.

`reduction` replaces the last *i* characters of every value, right-aligned to the value's own length, until all
values of the cluster are equal: `J96.11`, `J96.10` -> `J96.1*`; `J96.11`, `J96.01` -> `J96.**`. Values of
different length only become equal when replaced completely (`J96.11`, `J96.0` -> `******`), so it works best for
fixed-length codes (postcode prefixes, ICD-10 codes of one length). A missing value next to present ones also
leads to complete replacement; a cluster without any value releases `null`.

Bundled hierarchy files (`src/main/resources/hierarchies/`): `icd10_chapter1.yaml` (ICD-10 excerpt A00-A28),
`age_bands.yaml` (10-year bands `0-9` ... `90-99`, `100+` -> 20-year bands `0-19` ... `60-79`, `80+`), `rhythm.yaml`
(Philips monitor rhythm labels -> rhythm class -> group, use `root: Rhythm`).

Rules enforced when the profile is loaded: a quasi-identifier needs a generalizer; `l > 0` needs at least one
sensitive field; the data subject key must be `identifier-passthrough`, `mask` (hash) or `drop` (a dropped key is
used by Prink internally but not written); at most 24 non-dropped fields (Flink tuples have at most 25 fields,
one is used for `infoloss`); unknown keys are rejected.

Output records contain the configured, non-dropped fields plus `infoloss`. As before, values are written as
strings (generalized ranges look like `"(36.5,37.3)"`); missing values are written as JSON `null`.

Missing values: a missing/null quasi-identifier or sensitive value does not break the anonymization. Aggregation
ranges ignore it, a cluster with only missing values releases `null`, hierarchies treat it as a separate value
below the root, and for l-diversity "missing" counts as one distinct value.

### Running several profiles in parallel

Every profile is an independent Flink job; run one `flink run` per profile with its own output topic and group
id, e.g. as additional docker-compose services built from `og-prink/`:

```yaml
  og-prink-vitals:
    build: { context: ./og-prink }
    command: ["--profile", "vitals-sensitive"]      # appended to the image's `flink run ... prink-0.6.jar`
    environment:
      - PRINK_MASK_SALT=${PRINK_MASK_SALT}          # salt for masked fields, from the host environment
    depends_on: [flink-jobmanager, flink-taskmanager]
    networks: [kafka-network]

  og-prink-custom:
    build: { context: ./og-prink }
    command: ["--config", "/profiles/my-profile.yaml"]
    volumes: ["./og-prink/profiles:/profiles:ro"]
    depends_on: [flink-jobmanager, flink-taskmanager]
    networks: [kafka-network]
```

Each job needs one task slot (parallelism 1): raise `taskmanager.numberOfTaskSlots` in `flink-conf.yaml`
(currently 2: `flink-job` + one Prink job) or add task managers. Kafka creates the output topics automatically
(`auto.create.topics.enable`); consumers have to subscribe to the new topics (e.g. `prink-vitals-sensitive`).

## Usage
To use Prink add the `CastleFunction` as a process into your data stream using `.process()`:

	.process(new CastleFunction())

Make sure to key your data stream by the user identifier using the `.keyBy()` function (in the example below `user_id` is at field position 0):

	.keyBy(dataTuple -> dataTuple.getField(0))

Add a `ruleBroadcastStream` using the `.connect()` function to be able to communicate with Prink and to set rules:

	.connect(ruleBroadcastStream)

You can use your own implementation for the `ruleBroadcastStream` or if you only want to transfer some rules at startup a collection generated stream, as seen below.
Prink uses a `MapState` for rule broascasting:

	MapStateDescriptor<Integer, CastleRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "RulesBroadcastState",
                        BasicTypeInfo.INT_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CastleRule>(){}));

    ArrayList<CastleRule> rules = new ArrayList<>();
        rules.add(new CastleRule(0, new ReductionGeneralizer(), false));
        rules.add(new CastleRule(1, new AggregationFloatGeneralizer(Tuple2.of(17f, 90f)), false));
        rules.add(new CastleRule(2, new NoneGeneralizer(), true));


	BroadcastStream<CastleRule> ruleBroadcastStream = env
                .fromCollection(rules)
                .broadcast(ruleStateDescriptor);

Currently, it is needed to specify the return type of the data stream. Use the `.returns()` function to do so (change the `Tuple` size to the required size of your project).

	.returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object>>(){}));

The complete Prink integration should look something like this (excluding the rule broadcast stream):

	DataStream<Tuple4<Object, Object, Object, Object>> myDataStream = env
                .addSource(new MySource())

	DataStream<Tuple4<Object, Object, Object, Object>> myPrinkDataStream = myDataStream
                .keyBy(dataTuple -> dataTuple.getField(0))
                .connect(ruleBroadcastStream)
                .process(new CastleFunction())
                .returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, Object, Object,>>(){}));

To run your Flink job follow the offical Flink [documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/) or if you use the local installation run the following commands.

To start your Flink cluster run:

    $ ./bin/start-cluster.sh

To start your Flink job run:

	$ ./bin/flink run <your-flink-job>.jar

To stop your Flink cluster run:

	$ ./bin/stop-cluster.sh

---

To change the parameters of Prink add them when creating the `CastleFunction`:

	new CastleFunction(0, 50, 2, 200, 50, 50, 50, true, 2)

The parameters are the following:

| Name             | Type    | Default | Description                                                                                     |
|------------------|---------|---------|-------------------------------------------------------------------------------------------------|
| posTupleId       | int     | 0       | Position of the id value inside the tuples                                                      |
| k                | int     | 5       | Value k for k-anonymity                                                                         |
| l                | int     | 2       | Value l for l-diversity (if 0 l-diversity is not applied)                                       |
| delta            | int     | 20      | Number of 'delayed' tuples                                                                      |
| beta             | int     | 50      | Max number of clusters in bigGamma                                                              |
| zeta             | int     | 10      | Max number of clusters in bigOmega                                                              |
| mu               | int     | 10      | Number of last infoLoss values considered for tau                                               |
| showInfoLoss     | boolean | false   | If true Prink adds the info loss of a tuple at the end of the tuple (tuple size increases by 1) |
| suppressStrategy | int     | 2       | Defines how to handle tuples that need to be suppressed.                                        |

## Prink Rules
For the anonymization of the data stream Prink uses `CastleRules` to define how each data attribute should be handeld. These rules are broadcasted to the `CastleFunction` process and have the following structure:

| Name                   | Type                          | Allowed Values       | Usage                                                                    |
|------------------------|-------------------------------|----------------------|--------------------------------------------------------------------------|
| position               | int                           | 0-max size of Tuple  | Defines the position inside the data tuple that the rule should apply to |
| generalizer            | BaseGeneralizer               | BaseGeneralizer instance | Defines how the attribute at the defined position should be generalized |
| isSensibleAttribute    | boolean                       | [true, false]        | Defines if the position inside the data tuple is a sensible attribute    |
| infoLossMultiplier (Optional)| double                  | 0.0 - 1.0                  | Defines the multiplier for the Normalized Certainty Penalty calculation. If all rule values sum up to 1 the Normalized Certainty Penalty is used |

## Prink Generalizers
Currently, Prink provides the following Generalizers:

| Generalizer Class                   | Example Rule                                                                                                     | Generalization Result                                  |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|
| NoneGeneralizer                     | `new CastleRule(5, new NoneGeneralizer(), false)`                               | `[20, 22, 35]` &#8594; `[20, 22, 35]`              |
| ReductionGeneralizer                | `new CastleRule(5, new ReductionGeneralizer(), false)`                          | `[123456, 12789, 12678]` &#8594; `"12****"`           |
| AggregationIntegerGeneralizer       | `new CastleRule(5, new AggregationIntegerGeneralizer(Tuple2.of(0, 100)), false)` | `[20, 22, 35]` &#8594; `(20, 35)`                    |
| AggregationFloatGeneralizer         | `new CastleRule(5, new AggregationFloatGeneralizer(Tuple2.of(0f, 100f)), false)` | `[2.0f, 2.2f, 3.5f]` &#8594; `(2.0f, 3.5f)`          |
| AggregationDoubleGeneralizer        | `new CastleRule(5, new AggregationDoubleGeneralizer(Tuple2.of(0d, 100d)), false)`| `[2.0d, 2.2d, 3.5d]` &#8594; `(2.0d, 3.5d)`          |
| NonNumericalGeneralizer             | `new CastleRule(5, new NonNumericalGeneralizer(treeEntries), false)`            | `["IT-Student", "Bio-Student"]` &#8594; `"Student"` |

### Non-Numerical-Generalizer
To use the `NonNumericalGeneralizer` a domain generalization hierarchy needs to be defined.

This can be done in two ways:

1. sending an `Array` (`["Europe", "West-Europe","Germany"]`) instead of a `String` (`"Germany"`) as the data tuples attribute (This option is **not** encouraged and should only be used if necessary!).
1. providing it in the rule definition (**Preferred option**):

```
    ArrayList<String[]> treeNation = new ArrayList<>();
    treeNation.add(new String[]{"Europe", "South-Europe","Greece"});
    treeNation.add(new String[]{"Europe", "South-Europe","Italy"});
    treeNation.add(new String[]{"Europe", "South-Europe","Portugal"});
    treeNation.add(new String[]{"Europe", "West-Europe","England"});
    treeNation.add(new String[]{"Europe", "West-Europe","Germany"});
    treeNation.add(new String[]{"Asia", "West-Asia","Iran"});

    new CastleRule(14, new NonNumericalGeneralizer(treeNation), false)
```

## Custom Generalizer
Prink provides the option to add your own custom generalizer to the algorithm. This enables the use of new generalization concepts and the adaption to specific project needs.

To integrate a new Generalizer create a new Generalizer class that implements the `BaseGeneralizer`-Interface.
```
    public class MyCustomGeneralizer implements BaseGeneralizer{
        ...
    }
```

The Interface consists of 6 functions, that need to be implemented and have the following function:

### clone()
```
    BaseGeneralizer clone(int position, Cluster owningCluster);
```
The `clone()` function is part of the [prototype pattern](https://en.wikipedia.org/wiki/Prototype_pattern) used by Prink to create generalizer instances for newly created data clusters. When a new `Cluster` is created the `clone()` function will be called on the original generalizer instance provided in the defined `CastleRule` returning a new instance of the generalizer that only lives inside the new cluster.
The `clone()` function also provides the new generalizer instance with the `position` of the attribute that needs to be generalized, as well as a references to the `Cluster` that "owns" the generalizer instance. This references also enables the generalizer to have access to all data tuples inside the cluster by calling `getAllEntries()` on the cluster. 

For more information see: `BaseGeneralizer.java`

### generalize()
```
    Tuple2<?, Float> generalize();
```
The `generalize()` function is called when a data tuple needs to be generalized. 
It returns a `Tuple2` where the first value of the tuple is the generalization result of this generalizer and the second value is the resulting information loss of the applied generalization.
The generalization result can have any data type specified by the custom generalizer implementation. The information loss needs to be returned as a `Float` between `0f` (no information loss) and `1f` (all information is lost).

Note: Even though the `generalize()` function is called for each data tuple individually, the data tuple is not given to the `generalize()` function. This is to protect and ensure that the generalization is always applied over all data tuples inside the cluster, so no data can leak from an individual data tuple by accident.

**Important:** The generalization needs to happen over all data tuples inside the "owning" cluster and only these data tuples (`cluster.getAllEntries()`).
The function result is **not** allowed to be changed by the `generalize(withTuple)` function!

### generalize(withTuples)
```
    Tuple2<?, Float> generalize(List<Tuple> withTuples);
```
The `generalize(withTuple)` function is called when the enlargement value (the increase in information loss through a new data tuple in a cluster) needs to be calculated. 
The function should behave the exact same way as the normal `generalize()` function, except that the provided `List<Tuple>` of data tuples also needs to be included in the generalization result, returning a new generalization result and a new information loss.

**Important:** This inclusion of additional data tuples is only temporary and is not allowed to have any effect outside of the function call. The normal `generalize()` is not allowed to be affected by this function!

### generalizeMax()
```
    Tuple2<?, Float> generalizeMax();
```
The `generalizeMax()` function is called when a data tuple needs to be suppressed and should not just be set to `null`. This function should return the result of the maximal applied generalization of this generalizer, resulting in an information loss of `1f`.

Example: The `ReductionGeneralizer`s `generalizeMax()` function returns: `"*"` and the `AggregationIntegerGeneralizer`s `generalizeMax()` function with an upper and lower bound of `[0, 100]` returns: `(0, 100)`.

### addTuple(newTuple) - removeTuple(removedTuple)
```
    void addTuple(Tuple newTuple);
    void removeTuple(Tuple removedTuple);
```
The `addTuple(Tuple newTuple)` and `removeTuple(Tuple removedTuple)` function are helper functions that get called when a new data tuple is added or removed from the "owning" cluster. They make it possible to extract some of the generalizer logic out of the `generalize()` function and remove unnecessary calls and iterations over `cluster.getAllEntries()`.

Note: The use of these two functions is completely optional.

### Further information
If you need more information on custom generalizer creation, take a look at the Interface class: `BaseGeneralizer.java` and the default Prink generalizer implementations: `ReductionGeneralizer.java`, `AggregationIntGeneralizer.java`, `NonNumericalGeneralizer.java`.

[comment]: <> (## Contributing)

[comment]: <> (*WIP*)

## DEBUG

If you encounter the following error when starting a flink cluster and using Cygwin:

    Improperly specified VM option 'MaxMetaspaceSize=268435456

Follow the instructions here: https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/deployment/resource-providers/standalone/overview/#windows-cygwin-users

See also for additional error: https://stackoverflow.com/questions/72132420/why-apache-flink-in-not-running-on-windows

## License
The project will be open source. *WIP*
