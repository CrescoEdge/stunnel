package io.cresco.stunnel.state;

//%% NEW FILE SessionListenerSM BEGINS HERE %%

/*PLEASE DO NOT EDIT THIS CODE*/
/*This code was generated using the UMPLE 1.35.0.7523.c616a4dce modeling language!*/



// line 2 "model.ump"
// line 63 "model.ump"
public class SessionListenerSM
{

    //------------------------
    // MEMBER VARIABLES
    //------------------------

    //SessionListenerSM State Machines
    public enum State { initSessionListener, initForwardThread, activeForwardThread, waitCloseListener, closeSessionListener }
    private State state;

    //------------------------
    // CONSTRUCTOR
    //------------------------

    public SessionListenerSM()
    {
        setState(State.initSessionListener);
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
            case initSessionListener:
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
            case initSessionListener:
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
                setState(State.closeSessionListener);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean connectionBroken()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case activeForwardThread:
                setState(State.closeSessionListener);
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
                setState(State.closeSessionListener);
                wasEventProcessed = true;
                break;
            case waitCloseListener:
                setState(State.closeSessionListener);
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
                setState(State.waitCloseListener);
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
            case initSessionListener:
                // line 6 "model.ump"
                stateNotify("initSessionListener");
                break;
            case initForwardThread:
                // line 14 "model.ump"
                stateNotify("initForwardThread");
                break;
            case activeForwardThread:
                // line 22 "model.ump"
                stateNotify("activeForwardThread");
                break;
            case waitCloseListener:
                // line 31 "model.ump"
                stateNotify("waitCloseListener");
                break;
            case closeSessionListener:
                // line 37 "model.ump"
                stateNotify("closeSessionListener");
                break;
        }
    }

    public void delete()
    {}

    // line 43 "model.ump"
    public boolean stateNotify(String node){
        return true;
    }

}