package io.cresco.stunnel.state;

//%% NEW FILE SocketControllerSM BEGINS HERE %%

/*PLEASE DO NOT EDIT THIS CODE*/
/*This code was generated using the UMPLE 1.35.0.7523.c616a4dce modeling language!*/



// line 2 "model.ump"
// line 93 "model.ump"
public class SocketControllerSM
{

    //------------------------
    // MEMBER VARIABLES
    //------------------------

    //SocketControllerSM State Machines
    public enum State { pluginActive, initTunnelListener, activeTunnelListener, tunnelListenerRecovery, errorTunnelListener, initTunnelSender, activeTunnelSender, errorTunnelSender, pluginShutdown }
    private State state;

    //------------------------
    // CONSTRUCTOR
    //------------------------

    public SocketControllerSM()
    {
        setState(State.pluginActive);
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

    public boolean incomingSrcTunnelConfig()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case pluginActive:
                setState(State.initTunnelListener);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean incomingDstTunnelConfig()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case pluginActive:
                setState(State.initTunnelSender);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean startShutdown()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case pluginActive:
                setState(State.pluginShutdown);
                wasEventProcessed = true;
                break;
            case activeTunnelListener:
                setState(State.pluginShutdown);
                wasEventProcessed = true;
                break;
            case errorTunnelListener:
                setState(State.pluginShutdown);
                wasEventProcessed = true;
                break;
            case activeTunnelSender:
                setState(State.pluginShutdown);
                wasEventProcessed = true;
                break;
            case errorTunnelSender:
                setState(State.pluginShutdown);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean completeTunnelListenerInit()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case initTunnelListener:
                setState(State.activeTunnelListener);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean failedTunnelListenerInit()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case initTunnelListener:
                setState(State.pluginActive);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean srcFailure()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case activeTunnelListener:
                setState(State.errorTunnelListener);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean dstCommFailure()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case activeTunnelListener:
                setState(State.tunnelListenerRecovery);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean recoveredTunnel()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case tunnelListenerRecovery:
                setState(State.activeTunnelListener);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean failedRecoveredTunnel()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case tunnelListenerRecovery:
                setState(State.errorTunnelListener);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean completeTunnelSenderInit()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case initTunnelSender:
                setState(State.activeTunnelSender);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean failedTunnelSenderInit()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case initTunnelSender:
                setState(State.pluginActive);
                wasEventProcessed = true;
                break;
            default:
                // Other states do respond to this event
        }

        return wasEventProcessed;
    }

    public boolean dstFailure()
    {
        boolean wasEventProcessed = false;

        State aState = state;
        switch (aState)
        {
            case activeTunnelSender:
                setState(State.errorTunnelSender);
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
            case pluginActive:
                // line 5 "model.ump"
                stateNotify("pluginActive");
                break;
            case initTunnelListener:
                // line 16 "model.ump"
                stateNotify("initTunnelListener");
                break;
            case activeTunnelListener:
                // line 24 "model.ump"
                stateNotify("activeTunnelListener");
                break;
            case tunnelListenerRecovery:
                // line 32 "model.ump"
                stateNotify("tunnelListenerRecovery");
                break;
            case errorTunnelListener:
                // line 39 "model.ump"
                stateNotify("errorTunnelListener");
                break;
            case initTunnelSender:
                // line 48 "model.ump"
                stateNotify("initTunnelSender");
                break;
            case activeTunnelSender:
                // line 55 "model.ump"
                stateNotify("activeTunnelSender");
                break;
            case errorTunnelSender:
                // line 62 "model.ump"
                stateNotify("errorTunnelSender");
                break;
            case pluginShutdown:
                // line 68 "model.ump"
                stateNotify("pluginShutdown");
                break;
        }
    }

    public void delete()
    {}

    // line 73 "model.ump"
    public boolean stateNotify(String node){
        return true;
    }

}