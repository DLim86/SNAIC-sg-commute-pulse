# Recording Cards — open this as a VS Code tab and jump to the card you need

Cards exist so you never have to recall a number on camera.
Zoom VS Code to about 200% and show the card while you talk.

Verified against the database and the real `model.py --evaluate` output on 1 Sep 2026.

---

## CARD 1 — Bus headway gap


        Next bus            =   3 min
        The bus after that  =  20 min
        ------------------------------
        GAP                 =  17 min

        column name:  bus_headway_gap

        Same first number, completely different crowd.


---

## CARD 2 — The model redesign


        BEFORE
        Predict:  the whole journey time
        OneMap's own time was  87.5%  of the model
        -> the model was mostly copying OneMap
        -> and long routes hit a ceiling

        AFTER
        Predict:  the extra delay only
        Final answer = OneMap time + predicted delay
        OneMap's time is now  about 5%  of the model
        -> rush hour and bus timing matter more
        -> no more ceiling


---

## CARD 3 — Retry


        attempt 1  ->  fail  ->  wait 1 second
        attempt 2  ->  fail  ->  wait 2 seconds
        attempt 3  ->  fail  ->  give up

        the wait doubles each time
        = exponential backoff


---

## CARD 4 — Leave-by time


        leave_by  =  event start time
                     -  journey duration
                     -  10 min walking buffer


---

## CARD 5 — Model health (the real numbers)


        7-day average error, my model     =  6.1 min
        my warning threshold              = 10 min

        6.1 < 10   ->  dashboard says HEALTHY


        BUT my evaluator also checks the alternative:
        just trusting OneMap, with no model at all

        7-day average error, OneMap only  =  3.7 min

        3.7 < 6.1  ->  today my model does NOT beat it


---

## CARD 6 — Why that comparison is not fair yet


        Model score used   9 outcomes
          6 of them  = hand-logged by me on my phone
          3 of them  = reconstructed from OneMap's schedule

        OneMap score used only 3 outcomes
          all 3      = the reconstructed ones

        Those 3 "actuals" were built FROM OneMap's schedule.
        So OneMap is being scored against its own number.

        To make it fair I need hand-logged actuals for
        journeys that also have a route saved.


---

## CARD 7 — Honesty lines (say these exactly)


        "hand-logged observation"      NOT "actual journey time"
        "reconstructed estimate"       NOT "ground truth"
        "a warning signal"             NOT "proof of drift"

        I have no GPS tracking, so a true door-to-door
        time is not observable in this system.

