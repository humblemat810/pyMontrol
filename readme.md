TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT    TTTTTT
TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT     TTTTTT
TTTT            TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT    TTTTTT
TTTT    TTTT    TTTTTTTTTT  TTTTTTTTT TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT     TTTTTT
TTTT    TTTT    TTTTTTTTT   TTTTTTTT  TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT     TTTTTT
TTTT    TTTT    TTTTTTTT    TTTTTTT   TTTTTTTTTTTTTTTTTTTTTTTT                 TTTTTTTTTTTTTTTTT TTTTTTTTTTTT TT    TTTTT
TTTT            TTTTTTT     TTTTTT    TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT    TTTTTTTTTTTTTTTTTTTTTTT TTTTTTTTTTTT T    TTTTTT
TTTT    TTTTTTTTTTTTTT      TTTTT     TTTTTTTTTTTTTTTTTTTTTTTTTTTTTT   TTTTTTTTTTTTTTTTTTTTTTTT  TTTTTTTTTTTTT      TTTTTT
TTTT    TTTT    TTTTT       TTTT      TTTTTTTTTTTTTTTTTTTTTTTTTTTTTT   TTTTTTTTTTTTTTTTTTTTTTTT  TTTTTTTTTTTTT      TTTTTT
TTTT   TTTT    TTTTT   TT   TTT   T   TTT           TTT   TTTTTTTTTT  TTTTTTTTTTTTTTTTTTTTTTTTT  TTTTTTTTTTTT       TTTTTT
TTTT   TTT    TTTTT   TTT   TT   TT   TTT   TTTTTT  TTTT  T    TTTTT  TTTT  TT    TTTT     TTTTT TTTTTTTTTTT        TTTTTT
TTTT         TTTTT   TTTT   T   TTT   TT    TTTTT   TTTT  TTT   TTT   TTTTT    TTT TT  TTT  TTTT TTTTTTTTTTT         TTTTTT
YYYYYYYYY   TTTTT   TTTTT      TTTT   TT   TTTTT   TTTT  TTTT  TTTT   TTTTT  TTTTTTTT TTTTT TTTT TTTTTTTTTT          TTTTTT
TTTTTTTT    TTTT    TTTTT     TTTTT   TTT  TTTTT   TTT  TTTT  TTTTT  TTTTTT  TTTTTTTT  TTTT TTTT TTTTTTTTTT          TTTTTT
TTTTTT     TTTT    TTTTTT    TTTTTT   TTT  TTTTT  TTT  TTTTT TTTTT  TTTTTTT  TTTTTTTT  TTT TTTTT TTTTTTTTTT          TTTTTT
TTT       TTTT    TTTTTTT   TTTTTT   TTTTT       TTTT  TTTT  TTTTT  TTTTTTTT  TTTTTTT  TTT TTTTT TTTTTTTTT           TTTTTT
TTTTTTTTTTTTT     TTTTTT    TTTTT   TTTTTTTTTTTTTTTTT  TTTT   TTTT  TTTTTTT  TTTTTTTTT     TTTT  TTTTTTTTTTTT    TTTTTT
TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT    TTTTTT


PyMontrol is a library to control data flow using python and MongoDB

Design phylosophy:
Quick and clean way to achieve data flow that can sustain small to medium scale service deployment.
Large scale will be supported by hosting the worker and controller on to cloud functions
each worker work with 1 python interpreter



To use:
either 1a subscribe a mongodb.com replica set or 1.b self host a recplica set
2. change /src/credential/mongo_sample.json name to /src/credential/mongo.json with connection string properly set
3 .open a python console that run control.py
4 .after implementing worker.processdata and another python console that run worker.py, can be on same or distributed machines
the data uploaded to db = eventTrigger , collection name = input_data_packet will be allocated to the worker

Issues:
Very litter is done to worker monitor


