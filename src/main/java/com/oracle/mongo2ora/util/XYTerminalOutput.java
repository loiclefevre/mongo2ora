package com.oracle.mongo2ora.util;

import net.rubygrapefruit.platform.NativeException;
import net.rubygrapefruit.platform.terminal.TerminalOutput;
import net.rubygrapefruit.platform.terminal.TerminalSize;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class XYTerminalOutput {
	public static final String BOLD = "\u001b[1m";
	public static final String DIM_ON = "\u001b[2m";
	public static final String NORMAL_INTENSITY = "\u001b[22m";
	public static final String DEFAULT_FG = "\u001b[39m";
	public static final String DEFAULT_BG = "\u001b[49m";
	public static final String RESET = "\u001b[0m";
	public static final String START_OF_LINE = "\u001b[1G";
	public static final String CLEAR_TO_END_OF_LINE = "\u001b[0K";
	public static final String BrightRed= "\u001b[91m";
	public static final String BrightGreen = "\u001b[92m";
	public static final String BrightYellow= "\u001b[93m";

	private final TerminalOutput term;
	private int x;
	private int y;
	private int maxY;
	private static boolean WINDOWS = false;
	private TerminalOutput.Color background;

	public XYTerminalOutput(TerminalOutput terminal) {
		x = y = maxY = 0;
		this.term = terminal;
		final String osName = System.getProperty("os.name").toLowerCase();
		if (osName.startsWith("windows")) {
			WINDOWS = true;
		}
	}

	public OutputStream getOutputStream() {
		return term.getOutputStream();
	}

	public XYTerminalOutput write(CharSequence s) throws NativeException {
		x += s.length();
		term.write(s);
		return this;
	}

	public TerminalSize getTerminalSize() {
		return term.getTerminalSize();
	}

	public XYTerminalOutput reset() {
		term.reset();
		return this;
	}

	public XYTerminalOutput bold() {
		term.bold();
		return this;
	}

	public XYTerminalOutput newline() {
		term.newline();
		x = 0;
		y++;
		maxY = Math.max(maxY,y);
		return this;
	}

	public XYTerminalOutput foreground(TerminalOutput.Color color) {
		term.foreground(color);
		return this;
	}

	private static final List<String> BACKGROUND = new ArrayList<>();

	static {
		TerminalOutput.Color[] colors = TerminalOutput.Color.values();
		for (int i = 0; i < colors.length; ++i) {
			TerminalOutput.Color color = colors[i];
			BACKGROUND.add(String.format("\u001b[%sm", 40 + color.ordinal()));
		}
	}

	public XYTerminalOutput background(TerminalOutput.Color color) {
		term.write(BACKGROUND.get(color.ordinal()));
		this.background = color;
		return this;
	}

	public XYTerminalOutput defaultBackground() {
		term.write(DEFAULT_BG);
		this.background = null;
		return this;
	}

	public XYTerminalOutput underline() {
		term.write("\u001B[4;1m");
		return this;
	}

	public XYTerminalOutput bright() {
		term.bright();
		return this;
	}

	public int getX() {
		return x;
	}

	public int getY() {
		return y;
	}

	public XYTerminalOutput moveTo(int newX, int newY) {
		if (newX < x) {
			term.cursorLeft(x - newX);
		}
		else if (newX > x) {
			term.cursorRight(newX - x);
		}

		if (newY < y) {
			term.cursorUp(y - newY);
		}
		else if (newY > y) {
			term.cursorDown(newY - y);
		}

		x = newX;
		y = newY;

		maxY = Math.max(maxY,y);

		return this;
	}

	public XYTerminalOutput clearToEndOfLine() {
		term.clearToEndOfLine();
		return this;
	}

	public XYTerminalOutput hideCursor() {
		//TODO: via SubstrateVM only
		/*if(WINDOWS) {
			synchronized(term) {
				FunctionResult result = new FunctionResult();
				WindowsConsoleFunctions.hideCursor(result);
				if (result.isFailed()) {
					throw new NativeException(String.format("Could not hide cursor for %s: %s", term, result.getMessage()));
				}
			}
		} else {
			term.hideCursor();
		}*/
		return this;
	}

	public XYTerminalOutput showCursor() {
		/*if(WINDOWS) {
			synchronized(term) {
				FunctionResult result = new FunctionResult();
				WindowsConsoleFunctions.showCursor(result);
				if (result.isFailed()) {
					throw new NativeException(String.format("Could not show cursor for %s: %s", term, result.getMessage()));
				}			}
		} else {
			term.showCursor();
		}*/
		return this;
	}

	public XYTerminalOutput normal() {
		term.normal();
		return this;
	}

	public XYTerminalOutput moveToBottomScreen(int x) {
		moveTo(x,maxY);
		return this;
	}
}
