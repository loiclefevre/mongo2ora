package com.oracle.mongo2ora.asciigui;

import com.oracle.mongo2ora.util.Tools;
import com.oracle.mongo2ora.util.XYTerminalOutput;
import net.rubygrapefruit.platform.terminal.TerminalOutput;

public class ASCIICollectionProgressBar {

	private final int width;
	private final long startTime;
	private long targetJsonDocs;
	private long jsonDocs;
	private String docsString = "0 JSON docs";
	private String unit = "JSON docs";
	private TerminalOutput.Color speedColor;
	private double progressionPercentage;
	private int progressBackgroundLength;
	private int dashPosition;
	private boolean finished;
	private String timeCompleted;

	public ASCIICollectionProgressBar(int width, long startTime) {
		this.width = width;
		this.dashPosition = width / 2;
		this.startTime = startTime;
	}

	public void setProgressionPercentage(double percent) {
		this.progressionPercentage = percent;
		progressBackgroundLength = (int)Math.round((percent /100d)*(double)width);
	}

	public void write(XYTerminalOutput term) {
		int spacesBeforeSpeed = dashPosition - docsString.length() -1;
		final String time = timeCompleted == null ? Tools.getDurationSince(startTime) : timeCompleted;
		final int timeStartingPosition =dashPosition+2;
		final int timeEndingPosition = timeStartingPosition + time.length();

		// 16,170,80
		// 200,53,48
		for(int i = 0; i < width; i++) {
			if(i <= progressBackgroundLength) {
				//term.background(speedColor);
				term.write(getGradientColor(16,170,80,200,53,48,i,progressionPercentage));
			} else {
				term.defaultBackground();
			}

			if(i <= spacesBeforeSpeed) {
				term.write(" ");
			} else
			if(i < dashPosition ) {
				//term.bold().bright().foreground(TerminalOutput.Color.White).write("["+speedString+"]");
				term.bold().bright().foreground(TerminalOutput.Color.White).write(docsString.substring(i-spacesBeforeSpeed-1,i-spacesBeforeSpeed));
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

	public void addJSONDocs(long docNumber) {
		jsonDocs += docNumber;
		docsString = String.format("%,d %s",jsonDocs, unit);
		setProgressionPercentage((double)(100*jsonDocs) / (double)targetJsonDocs);
	}

	public void addTargetJSONDocs(long docNumber) {
		targetJsonDocs += docNumber;
		setProgressionPercentage((double)(100*jsonDocs) / (double)targetJsonDocs);
	}
}
