package timeseries

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	MINUTE = 60
	HOUR   = 3600
	DAY    = 24 * HOUR
	WEEK   = 7 * DAY
)

type uomConversion struct {
	uom        string
	multiplier float64
}

var uom_conversion map[string]uomConversion
var uom_mapping map[string]string

func init() {
	uom_conversion = map[string]uomConversion{
		"B": uomConversion{
			uom:        "bytes",
			multiplier: 1,
		},
		"KB": uomConversion{
			uom:        "bytes",
			multiplier: 1000,
		},
		"MB": uomConversion{
			uom:        "bytes",
			multiplier: 1000 * 1000,
		},
		"GB": uomConversion{
			uom:        "bytes",
			multiplier: 1000 * 1000 * 1000,
		},
		"TB": uomConversion{
			uom:        "bytes",
			multiplier: 1000 * 1000 * 1000 * 1000,
		},
		"s": uomConversion{
			uom:        "seconds",
			multiplier: 1,
		},
		"ms": uomConversion{
			uom:        "seconds",
			multiplier: 1.0 / 1000.0,
		},
		"us": uomConversion{
			uom:        "seconds",
			multiplier: 1.0 / 1000000.0,
		},
		"%": uomConversion{
			uom:        "percent",
			multiplier: 1,
		},
	}

	uom_mapping = map[string]string{
		"Bytes": "B", // some plugins return back Bytes for B - grrrr!
		"B":     "B",
		"KB":    "KB",
		"M":     "MB", // nsclient returns back M for MB - grrrrr!
		"MB":    "MB",
		"GB":    "GB",
		"TB":    "TB",
		"s":     "s",
		"ms":    "ms",
		"us":    "us",
		"%":     "%",
	}
}

func ConvertUom(uom string) (new_uom string, multiplier float64) {
	new_uom = uom
	multiplier = 1
	if v, ok := uom_mapping[uom]; ok {
		if c, ok := uom_conversion[v]; ok {
			new_uom = c.uom
			multiplier = c.multiplier
		}
	}

	return
}

func CalculateTimeSlotSize(datapoints int64, startEpoch int64, endEpoch int64, minSlotSize float64, fixedSlotSize float64) string {
	timeDiff := endEpoch - startEpoch
	if timeDiff > datapoints {
		slotSizeSec := float64(timeDiff) / float64(datapoints)
		if fixedSlotSize > 0 {
			slotSizeSec = fixedSlotSize
		}
		if slotSizeSec < minSlotSize {
			slotSizeSec = minSlotSize
		}
		if slotSizeSec < 1 {
			slotSizeSec = 1
		}
		switch {
		case slotSizeSec < MINUTE:
			return fmt.Sprintf("%ds", int(slotSizeSec))
		case slotSizeSec < HOUR:
			return fmt.Sprintf("%dm", int(slotSizeSec/MINUTE))
		case slotSizeSec < DAY:
			return fmt.Sprintf("%dh", int(slotSizeSec/HOUR))
		case slotSizeSec < WEEK:
			return fmt.Sprintf("%dd", int(slotSizeSec/DAY))
		default:
			return fmt.Sprintf("%dw", int(slotSizeSec/WEEK))
		}
	}
	return "1s"
}

func CheckFillOption(value, default_value string) (result string, err error) {
	err = nil
	result = default_value

	switch value {
	case "":
		return
	case "linear", "none", "null", "previous":
		result = value
	default:
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			result = value
		} else {
			err = errors.New("Invalid value")
		}
	}
	return
}
