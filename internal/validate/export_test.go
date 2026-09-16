package validate

// Test-only handles on the unexported helpers the unit specs pin
// (longestGap, daysInMonth, the period parsers, monthFromDate), so the
// external spec package can characterize them without the package
// exporting them.
var (
	LongestGap       = longestGap
	DaysInMonth      = daysInMonth
	PeriodBounds     = periodBounds
	PeriodYearBounds = periodYearBounds
	MonthFromDate    = monthFromDate
)
