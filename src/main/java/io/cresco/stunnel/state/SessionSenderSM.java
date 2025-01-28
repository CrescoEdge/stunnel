package io.cresco.stunnel.state;

//%% NEW FILE SessionSenderSM BEGINS HERE %%

/*PLEASE DO NOT EDIT THIS CODE*/
/*This code was generated using the UMPLE 1.35.0.7523.c616a4dce modeling language!*/



// line 2 "model.ump"
// line 46 "model.ump"
public class SessionSenderSM
{

    //------------------------
    // MEMBER VARIABLES
    //------------------------

    //SessionSenderSM State Machines
    public enum State { initSessionSender, initForwardThread, activeForwardThread, waitCloseSender, closeSessionSender, closeSessionListener }
    private State state;

    //------------------------
    // CONSTRUCTOR
    //------------------------

    public SessionSenderSM()
    {
        setState(State.initSessionSender);
    }

    //------------------------
    // INTERFACE
    //------------------------

    public String getStateFullName()
    {
        String answer = state.toString();
        return answer;
    }

    public State getState()
    {
        return state;
    }

    public boolean createForwardThread()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case initSessionSender:
                setState(State.initForwardThread);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean failedCreateForwardThread()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case initSessionSender:
                setState(State.closeSessionListener);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean startedFowardThread()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case initForwardThread:
                setState(State.activeForwardThread);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean failedStartFowardThread()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case initForwardThread:
                setState(State.closeSessionSender);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean dstClose()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case activeForwardThread:
                setState(State.closeSessionSender);
                wasEventProcessed = true;
                break;
            case waitCloseSender:
                setState(State.closeSessionSender);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean srcClose()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case activeForwardThread:
                setState(State.waitCloseSender);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    private void setState(State aState)
    {
        state = aState;

        // entry actions and do activities
        switch(state)
        {
            case initSessionSender:
                // line 6 "model.ump"
                stateNotify("initSessionSender");
                break;
            case initForwardThread:
                // line 14 "model.ump"
                stateNotify("initForwardThread");
                break;
            case activeForwardThread:
                // line 22 "model.ump"
                stateNotify("activeForwardThread");
                break;
            case waitCloseSender:
                // line 29 "model.ump"
                stateNotify("waitCloseSender");
                break;
            case closeSessionSender:
                // line 35 "model.ump"
                stateNotify("closeSessionSender");
                break;
        }
    }

    public void delete()
    {}

    // line 41 "model.ump"
    public boolean stateNotify(String node){
        return true;
    }

}

