![TT](<img/logo.png>)

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


Detailed Tutorial:

![TT](<img/logo.png>)
figure 1  Settings for /credential/mongo.json
This library make use of the event stream to achieve control. So in mongodb a replicaset is required.
As seen in figure 1, the mongodb need a connection string to be connected to the mongodb server.

After configuration, we just need to run control.py and worker.py to get it work.
However, what to do with the data is up to your own implementation althought a sample local control is partially implemented

![TT](<img/run_controller_and_worker.png>)
figure2 run both worker and controller and they start working

![TT](<img/example_process_data.png>)
figure3 example implementation of data without using the local control
If you would like to use the local control, use the commented code and edit the function main_control_loop

![TT](<img/compassView.png>)
Open mongodb compass and see the worker and controllers are working


Enjoy control with pure python without using Flask

 TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT    TTTTTT
 TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT     TTTTT
 TTTT            TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT    TTTTT
 TTTT    TTTT    TTTTTTTTTT  TTTTTTTTT TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT     TTTTT
 TTTT    TTTT    TTTTTTTTT   TTTTTTTT  TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT     TTTTTT
 TTTT    TTTT    TTTTTTTT    TTTTTTT   TTTTTTTTTTTTTTTTTTTTTTT                 TTTTTTTTTTTTTTTTT TTTTTTTTTTT TT    TTTTTT
 TTTT            TTTTTTT     TTTTTT    TTTTTTTTTTTTTTTTTTTTTTTTTTTTTT    TTTTTTTTTTTTTTTTTTTTTTT TTTTTTTTTTT T    TTTTTTT
 TTTT    TTTTTTTTTTTTTT      TTTTT     TTTTTTTTTTTTTTTTTTTTTTTTTTTTT   TTTTTTTTTTTTTTTTTTTTTTTT  TTTTTTTTTTTTT     TTTTTT
 TTTT    TTTT    TTTTT       TTTT      TTTTTTTTTTTTTTTTTTTTTTTTTTTTT   TTTTTTTTTTTTTTTTTTTTTTTT  TTTTTTTTTTTTT      TTTTT
 TTTT   TTTT    TTTTT   TT   TTT   T   TTT           TT   TTTTTTTTTT  TTTTTTTTTTTTTTTTTTTTTTTTT  TTTTTTTTTTTT       TTTTT
 TTTT   TTT    TTTTT   TTT   TT   TT   TTT   TTTTTT  TTT  T    TTTTT  TTTT  TT    TTTT     TTTTT TTTTTTTTTTTT       TTTTT
 TTTT         TTTTT   TTTT   T   TTT   TT    TTTTT   TTT  TTT   TTT   TTTTT    TTT TT  TTT  TTTT TTTTTTTTTTT         TTTT
 TTTTTTTTTT   TTTTT   TTTTT      TTTT   TT   TTTTT   TTT  TTTT  TTTT   TTTTT  TTTTTTTT TTTTT TTTT TTTTTTTTTT         TTTT
 TTTTTTTT    TTTT    TTTTT     TTTTT   TTT  TTTTT   TTT  TTTT  TTTTT  TTTTTT  TTTTTTTT  TTTT TTTT TTTTTTTTTT          TTT
 TTTTTT     TTTT    TTTTTT    TTTTTT   TTT  TTTTT  TTT  TTTTT TTTTT  TTTTTTT  TTTTTTTT  TTT TTTTT TTTTTTTTTT          TTT
 TTT       TTTT    TTTTTTT   TTTTTT   TTTTT       TTTT  TTTT  TTTTT  TTTTTTTT  TTTTTTT  TTT TTTTT TTTTTTTTT           TTT
 TTTTTTTTTTTTT     TTTTTT    TTTTT   TTTTTTTTTTTTTTTTT  TTTT   TTTT  TTTTTTT  TTTTTTTTT     TTTT  TTTTTTTTTTTT    TTTTTTT
 TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT    TTTTTTTT