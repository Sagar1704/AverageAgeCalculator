package bigdata.sea.reducejoin;

public class User implements Comparable<User> {
	private String userId;
	private String address;
	private double averageAge;

	public User() {
	}

	public User(String userId, String address, double averageAge) {
		super();
		this.userId = userId;
		this.address = address;
		this.averageAge = averageAge;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public double getAverageAge() {
		return averageAge;
	}

	public void setAverageAge(double averageAge) {
		this.averageAge = averageAge;
	}

	@Override
	public int compareTo(User user) {
		if (this.getAverageAge() > user.getAverageAge())
			return -1;
		else if (this.getAverageAge() == user.getAverageAge() && Long
				.parseLong(this.getUserId()) < Long.parseLong(user.getUserId()))
			return -1;
		return 1;
	}

	@Override
	public String toString() {
		return userId + ", " + address + ", " + averageAge;
	}

}
