package com.oracle.mongo2ora.asciigui;

import com.oracle.mongo2ora.util.Tools;
import com.oracle.mongo2ora.util.XYTerminalOutput;
import net.rubygrapefruit.platform.terminal.TerminalOutput;

public class ASCIIProgressBar {

	private final int width;
	private final long startTime;
	private double speed;
	private double maxSpeed;
	private String speedString = "0.0 MB/s";
	private String maxSpeedString = " (max: 0.0 MB/s)";
	private String unit = "MB/s";
	private String maxUnit = "MB/s";
	private TerminalOutput.Color speedColor;
	private double progressionPercentage;
	private int progressBackgroundLength;
	private int dashPosition;
	private boolean finished;
	private String timeCompleted;

	public ASCIIProgressBar(int width, long startTime) {
		this.width = width;
		this.dashPosition = 2 * width / 3;
		this.startTime = startTime;
	}

	public TerminalOutput.Color setSpeed(double speedInMBPerSec, double maxSpeedInMBPerSec) {
		this.speed = speedInMBPerSec;
		if(speed <= 1024d) {
			unit = "MB/s";
			speedString = String.format("%.1f MB/s",speed);
		} else {
			speed /= 1024d;
			unit = "GB/s";
			speedString = String.format("%.1f GB/s",speed);
		}

		this.maxSpeed = maxSpeedInMBPerSec;
		if(maxSpeed <= 1024d) {
			maxUnit = "MB/s";
			maxSpeedString = String.format(" (max: %.1f MB/s)",maxSpeed);
		} else {
			maxSpeed /= 1024d;
			maxUnit = "GB/s";
			maxSpeedString = String.format(" (max: %.1f GB/s)",maxSpeed);
		}


		//System.out.println(speedInMBPerSec+", "+speedString);

		if ("0.0".equals(String.format("%.1f", speed))) {
			speedColor = TerminalOutput.Color.White;
		}
		else if (speedInMBPerSec < 512) {
			speedColor = TerminalOutput.Color.Green;
		}
		else if (speedInMBPerSec < 1024) {
			speedColor = TerminalOutput.Color.Yellow;
		}
		else {
			speedColor = TerminalOutput.Color.Red;
		}

		return speedColor;
	}

	public void setProgressionPercentage(double percent) {
		this.progressionPercentage = percent;
		progressBackgroundLength = (int)Math.round((percent /100d)*(double)width);
	}

	public void write(XYTerminalOutput term) {
		int spacesBeforeSpeed = dashPosition - speedString.length() -maxSpeedString.length() -1;
		final String time = timeCompleted == null ? Tools.getDurationSince(startTime) : timeCompleted;
		final int timeStartingPosition =dashPosition+2;
		final int timeEndingPosition = timeStartingPosition + time.length();

		// 16,170,80
		// 200,53,48
		for(int i = 0; i < width; i++) {
			if(i <= progressBackgroundLength) {
				term.background(speedColor);
				//term.write(getGradientColor(16,170,80,200,53,48,i,progressionPercentage));
			} else {
				term.defaultBackground();
			}

			if(i <= spacesBeforeSpeed) {
				term.write(" ");
			} else
			if(i < dashPosition ) {
				//term.bold().bright().foreground(TerminalOutput.Color.White).write("["+speedString+"]");
				if(i < dashPosition - maxSpeedString.length()) {
					term.bold().bright().foreground(TerminalOutput.Color.White).write( speedString.substring(i - spacesBeforeSpeed - 1, i - spacesBeforeSpeed));
				} else {
					term.normal().write(String.format("\u001b[38;2;%d;%d;%dm",255,203,107)).write(maxSpeedString.substring(i - spacesBeforeSpeed - 1 - speedString.length(), i - spacesBeforeSpeed - speedString.length()));
				}
				//term.normal().foreground(TerminalOutput.Color.Cyan).write(maxSpeedString);
			} else
			if(i == dashPosition) {
				term.normal().foreground(TerminalOutput.Color.White).write(" ");
			}else
			if(i == dashPosition+1) {
				term.bold().bright().foreground(TerminalOutput.Color.White).write("-");
			}else
			if(i == dashPosition+2) {
				term.normal().foreground(TerminalOutput.Color.White).write(" ");
			} else
			if(i <= timeEndingPosition)
			{
				term.bold().bright().foreground(TerminalOutput.Color.White).write(time.substring(i-timeStartingPosition-1,i-timeStartingPosition));
			} else {
				term.write(" ");
			}
		}
	}

	private String getGradientColor(int fr, int fg, int fb, int tr, int tg, int tb, int i, double progressionPercentage) {
		if(progressionPercentage == 0) return "";

		if(i == 0) return String.format("\u001b[48;2;%d;%d;%dm",fr,fg,fb);

		if(i == width) return String.format("\u001b[48;2;%d;%d;%dm",tr,tg,tb);

		final double rInc = (tr - fr) / (double)width;
		final double gInc = (tg - fg) / (double)width;
		final double bInc = (tb - fb) / (double)width;

		int rd = (int)(fr+rInc * i);
		int gd = (int)(fg+gInc * i);
		int bd = (int)(fb+bInc * i);

		return String.format("\u001b[48;2;%d;%d;%dm",rd,gd,bd);
	}

	public boolean isFinished() {
		return finished;
	}

	public void finish() {
		progressionPercentage = 100d;
		progressBackgroundLength = width;
		timeCompleted=Tools.getDurationSince(startTime);
		finished = true;
	}
}
