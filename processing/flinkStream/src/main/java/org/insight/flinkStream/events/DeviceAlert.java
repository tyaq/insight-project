package org.insight.flinkStream.events;

public class DeviceAlert {
    private int deviceID;

    public TemperatureAlert(int rackID) {
      this.rackID = rackID;
    }

    public TemperatureAlert() {
      this(-1);
    }

    public void setRackID(int rackID) {
      this.rackID = rackID;
    }

    public int getRackID() {
      return rackID;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TemperatureAlert) {
        TemperatureAlert other = (TemperatureAlert) obj;
        return rackID == other.rackID;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return rackID;
    }

    @Override
    public String toString() {
      return "TemperatureAlert(" + getRackID() + ")";
    }
}
