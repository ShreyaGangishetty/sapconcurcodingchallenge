package SAPConcurCodingChallenge;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class CheckDuplicateEvent {

    Timer timerForDeleteingHashMapElements;
    //for every 60 seconds invalidate the hashmap events as expired and remove those events
    private static long EXPIRED_TIME_SECS = 60;
    //has the details of events
    private static Map<String, List<Date>> eventsHMap = new HashMap<>();

    public void processEvent(String EventID, String EventBody){
        //checks if duplicate event is found
        if(eventsHMap.containsKey(EventID)) {
            List<Date> newArrDates = eventsHMap.get(EventID);
            newArrDates.add(new Date());
            //if a duplicate is found the duplicate event ID's date is also added (we can also remove the current timestamp of event id and add the new event timestamp)
            eventsHMap.put(EventID, newArrDates);
            System.out.println("This is a duplicate event");
        }
        else{
            processEventWithoutDuplicates(EventID, EventBody);
            System.out.println("Process even without duplicates is called as the event ID is not present in the map");
            Date currTime = new Date();
            ArrayList<Date> arrayDates = new ArrayList<Date>();
            arrayDates.add(currTime);
            //adding the eventID with its insertionTime as currentTime in hashMap
            eventsHMap.put(EventID, arrayDates);
        }

    }

    /** This function removes expired eventIDs and dates from the Map */
    private static void removeExpiredEventsFromMap(Map<String, List<Date>> map) {
        Date currentTime = new Date();
        Date actualExpiredTime = new Date(currentTime.getTime() - EXPIRED_TIME_SECS * 1000l);
        Iterator<Entry<String, List<Date>>> eventsMapIterator = map.entrySet().iterator();
        //iterating over the events map and removing all the expired dates
        while (eventsMapIterator.hasNext()) {
            Entry<String, List<Date>> eventRecord = eventsMapIterator.next();
            List<Date> eventDates = eventRecord.getValue();
            //filters dates which are still valid for a particular event id of event map and creates a new list
            List<Date> unExpiredEventDates = eventDates
                                                .stream()
                                                .filter(element->element.compareTo(actualExpiredTime)>0)
                                                .collect(Collectors.toList());
            //if the new list is not empty it means that the event ID is still within the window of unexpired events
            if(!unExpiredEventDates.isEmpty())
                eventsHMap.put(eventRecord.getKey(),unExpiredEventDates);
            else
                eventsHMap.remove(eventRecord.getKey());
        }
    }

    public static void main(String args[]) {
        //this is used here to check and clean the event map at regular intervals of 20 seconds
        new CheckDuplicateEvent().removeExpiredEventMapElements(20);
    }

    public void removeExpiredEventMapElements(int seconds) {
        timerForDeleteingHashMapElements = new Timer();
        //schedules the delete events of event map for every 20 seconds // this can be modified
        timerForDeleteingHashMapElements.schedule(new DeleteEventReminder(), 0, seconds * 1000);
    }

    class DeleteEventReminder extends TimerTask {
        public void run() {
            synchronized(eventsHMap) {
                //every 20 seconds this function gets called and the clean up for event map is done
                //used synchronized here to avoid concurrent add, delete or update of hashmap
                removeExpiredEventsFromMap(eventsHMap);
            }
        }
    }

    public void processEventWithoutDuplicates(String EventID, String EventBody){
        // this does something very complicated that we do not what do touch
    }

}
/*
example
processEvent(1,"")
processEvent(2,"")
processEvent(3,"")
processEvent(4,"")
processEvent(1,"")
//expected result
processEventWithoutDuplicates(1,"")
processEventWithoutDuplicates(2,"")
processEventWithoutDuplicates(3,"")
processEventWithoutDuplicates(4,"")
*/
