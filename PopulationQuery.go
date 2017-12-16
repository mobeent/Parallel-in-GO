package main

import (
    "fmt"
    "os"
    "strconv"
		"sync"
    "math"
	"encoding/csv"
)

const (
	SEQUENTIAL_CUTOFF = 20
	GRID_CUTOFF = 400
)

type Rectangle struct {
	left, right, top, bottom float64
}

func (this Rectangle) encompass(that Rectangle) (Rectangle) {
	r1 := Rectangle{
		math.Min(this.left, that.left),
		math.Max(this.right, that.right),
		math.Max(this.top, that.top),
		math.Min(this.bottom, that.bottom)}

	return r1
}

func (this Rectangle) toString() (string) {
	return "[left=" + strconv.FormatFloat(this.left, 'f', -1, 64) +
	" right=" + strconv.FormatFloat(this.right, 'f', -1, 64) +
	" top=" + strconv.FormatFloat(this.top, 'f', -1, 64) +
	" bottom=" + strconv.FormatFloat(this.bottom, 'f', -1, 64) +
	"]"
}

type CensusGroup struct {
	population int
	latitude, longitude float64
}

func ParseCensusData(fname string) ([]CensusGroup, int, error) {
	file, err := os.Open(fname)
    totalpop := 0;
    if err != nil {
		return nil, 0, err
    }
    defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, 0, err
	}
	censusData := make([]CensusGroup, 0, len(records))

    for _, rec := range records {
        if len(rec) == 7 {
            population, err1 := strconv.Atoi(rec[4])
            totalpop = totalpop + population
            latitude, err2 := strconv.ParseFloat(rec[5], 64)
            longitude, err3 := strconv.ParseFloat(rec[6], 64)
            if err1 == nil && err2 == nil && err3 == nil {
                latpi := latitude * math.Pi / 180
                latitude = math.Log(math.Tan(latpi) + 1 / math.Cos(latpi))
                censusData = append(censusData, CensusGroup{population, latitude, longitude})
            }
        }
    }

	return censusData, totalpop, nil
}

func main () {
	if len(os.Args) < 4 {
		fmt.Printf("Usage:\nArg 1: file name for input data\nArg 2: number of x-dim buckets\nArg 3: number of y-dim buckets\nArg 4: -v1, -v2, -v3, -v4, -v5, or -v6\n")
		return
	}
	fname, ver := os.Args[1], os.Args[4]
    xdim, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}
    ydim, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println(err)
		return
	}
	censusData, totalpop, err := ParseCensusData(fname)
	if err != nil {
		fmt.Println(err)
		return
	}

	corners := Rectangle{censusData[0].longitude, censusData[0].longitude, censusData[0].latitude, censusData[0].latitude}

	grid := [][]int{}
	for i := 0; i < ydim; i++ {
		tempsplice := make([]int, xdim, xdim)
		grid = append(grid, tempsplice)
	}

    // Some parts may need no setup code
    switch ver {
    case "-v1":
        // YOUR SETUP CODE FOR PART 1
				for _, cen := range censusData {
					temp := Rectangle{cen.longitude, cen.longitude, cen.latitude, cen.latitude}
					corners = corners.encompass(temp)
				}
				fmt.Printf("Total Population: %v\n The boundaries for data: %s\n", totalpop, corners.toString())

    case "-v2":
        // YOUR SETUP CODE FOR PART 2
				parent := make(chan Rectangle)
				go parallelrectangle(censusData, 0, len(censusData), parent)
				corners = <-parent
				fmt.Printf("Total Population: %v\n The boundaries for data: %s\n", totalpop, corners.toString())

    case "-v3":
        // YOUR SETUP CODE FOR PART 3
				for _, cen := range censusData {
					temp := Rectangle{cen.longitude, cen.longitude, cen.latitude, cen.latitude}
					corners = corners.encompass(temp)
				}
				fmt.Printf("Total Population: %v\n The boundaries for data: %s\n", totalpop, corners.toString())

				for _, cen := range censusData {
					index2 := int(((cen.longitude - corners.left) / (corners.right - corners.left)) * float64(xdim))
					index1 := int(((cen.latitude - corners.bottom) / (corners.top - corners.bottom)) * float64(ydim))
					if (index2 == xdim) {
						index2 = index2 - 1
					}
					if (index1 == ydim) {
						index1 = index1 - 1
					}
					grid[index1][index2] = grid[index1][index2] + cen.population
				}

				for j := ydim-1; j >= 0; j-- {
					for i := 0; i < xdim; i++ {
						if ((i-1) >= 0) {
							grid[j][i] = grid[j][i] + grid[j][i-1]
						}
						if ((j+1) < ydim) {
							grid[j][i] = grid[j][i] + grid[j+1][i]
						}
						if ((i-1) >= 0 && (j+1) < ydim) {
							grid[j][i] = grid[j][i] - grid[j+1][i-1]
						}
					}
				}

    case "-v4":
        // YOUR SETUP CODE FOR PART 4
				parent := make(chan Rectangle)
				go parallelrectangle(censusData, 0, len(censusData), parent)
				corners = <-parent
				fmt.Printf("Total Population: %v\n The boundaries for data: %s\n", totalpop, corners.toString())

				parent2 := make(chan [][]int)
				go parallelgrid(censusData, corners, xdim, ydim, 0, len(censusData), parent2)
				grid = <-parent2

				for j := ydim-1; j >= 0; j-- {
					for i := 0; i < xdim; i++ {
						if ((i-1) >= 0) {
							grid[j][i] = grid[j][i] + grid[j][i-1]
						}
						if ((j+1) < ydim) {
							grid[j][i] = grid[j][i] + grid[j+1][i]
						}
						if ((i-1) >= 0 && (j+1) < ydim) {
							grid[j][i] = grid[j][i] - grid[j+1][i-1]
						}
					}
				}

    case "-v5":
        // YOUR SETUP CODE FOR PART 5
				locks := [][]sync.Mutex{}
				for i := 0; i < ydim; i++ {
					tempsplice := make([]sync.Mutex, xdim, xdim)
					locks = append(locks, tempsplice)
				}

				parent := make(chan Rectangle)
				go parallelrectangle(censusData, 0, len(censusData), parent)
				corners = <-parent
				fmt.Printf("Total Population: %v\n The boundaries for data: %s\n", totalpop, corners.toString())

				parent2 := make(chan [][]int)
				go parallelgridwithlock(grid, locks, censusData, corners, xdim, ydim, 0, len(censusData), parent2)
				grid = <-parent2

				for j := ydim-1; j >= 0; j-- {
					for i := 0; i < xdim; i++ {
						if ((i-1) >= 0) {
							grid[j][i] = grid[j][i] + grid[j][i-1]
						}
						if ((j+1) < ydim) {
							grid[j][i] = grid[j][i] + grid[j+1][i]
						}
						if ((i-1) >= 0 && (j+1) < ydim) {
							grid[j][i] = grid[j][i] - grid[j+1][i-1]
						}
					}
				}


    case "-v6":
        // YOUR SETUP CODE FOR PART 6
    default:
        fmt.Println("Invalid version argument")
        return
    }

    for {
        var west, south, east, north int
        n, err := fmt.Scanln(&west, &south, &east, &north)
        if n != 4 || err != nil || west<1 || west>xdim || south<1 || south>ydim || east<west || east>xdim || north<south || north>ydim {
            break
        }

				leftborder := ((float64(west-1) / float64(xdim)) * (corners.right - corners.left)) + corners.left
				bottomborder := ((float64(south-1) / float64(ydim)) * (corners.top - corners.bottom)) + corners.bottom
				rightborder := ((float64(east) / float64(xdim)) * (corners.right - corners.left)) + corners.left
				topborder := ((float64(north) / float64(ydim)) * (corners.top - corners.bottom)) + corners.bottom

        var population int
        var percentage float64
        switch ver {
        case "-v1":
            // YOUR QUERY CODE FOR PART 1
						for _, cen := range censusData {
							if (cen.longitude >= leftborder && cen.longitude <= rightborder && cen.latitude >= bottomborder && cen.latitude <= topborder) {
								population = population + cen.population
							}
						}

        case "-v2":
            // YOUR QUERY CODE FOR PART 2
						parent := make(chan int)
						go parallelquery(censusData, 0, len(censusData), leftborder, bottomborder, rightborder, topborder, parent)
						population = <-parent

        case "-v3":
            // YOUR QUERY CODE FOR PART 3
						population = population + grid[south-1][east-1]
						if (north < ydim) {
							population = population - grid[north][east-1]
						}
						if ((west-2) >= 0) {
							population = population - grid[south-1][west-2]
						}
						if (north < ydim && (west-2) >= 0) {
							population = population + grid[north][west-2]
						}

        case "-v4":
            // YOUR QUERY CODE FOR PART 4
						population = population + grid[south-1][east-1]
						if (north < ydim) {
							population = population - grid[north][east-1]
						}
						if ((west-2) >= 0) {
							population = population - grid[south-1][west-2]
						}
						if (north < ydim && (west-2) >= 0) {
							population = population + grid[north][west-2]
						}

        case "-v5":
            // YOUR QUERY CODE FOR PART 5
						population = population + grid[south-1][east-1]
						if (north < ydim) {
							population = population - grid[north][east-1]
						}
						if ((west-2) >= 0) {
							population = population - grid[south-1][west-2]
						}
						if (north < ydim && (west-2) >= 0) {
							population = population + grid[north][west-2]
						}

        case "-v6":
            // YOUR QUERY CODE FOR PART 6
        }

				percentage = (float64(population)/float64(totalpop)) * 100.0
        fmt.Printf("%v %.2f%%\n", population, percentage)
    }
}

func parallelrectangle(censusData []CensusGroup, lo int, hi int, parent chan Rectangle) {
	if (hi - lo < SEQUENTIAL_CUTOFF) {
		corners := Rectangle{censusData[0].longitude, censusData[0].longitude, censusData[0].latitude, censusData[0].latitude}
		for i := lo; i < hi; i++ {
			temp := Rectangle{censusData[i].longitude, censusData[i].longitude, censusData[i].latitude, censusData[i].latitude}
			corners = corners.encompass(temp)
		}
		parent<- corners
	} else {
			leftch := make(chan Rectangle)
			rightch := make(chan Rectangle)
			go parallelrectangle(censusData, lo, (hi+lo)/2, leftch)
			go parallelrectangle(censusData, (hi+lo)/2, hi, rightch)
			rightAns := <-rightch
			leftAns := <-leftch
			parent<- leftAns.encompass(rightAns);
	}
}

func parallelquery(censusData []CensusGroup, lo int, hi int, leftborder float64, bottomborder float64, rightborder float64, topborder float64, parent chan int) {
	if (hi - lo < SEQUENTIAL_CUTOFF) {
		population := 0
		for i := lo; i < hi; i++ {
			if (censusData[i].longitude >= leftborder && censusData[i].longitude <= rightborder && censusData[i].latitude >= bottomborder && censusData[i].latitude <= topborder) {
				population = population + censusData[i].population
			}
		}
		parent<- population
	} else {
			leftch := make(chan int)
			rightch := make(chan int)
			go parallelquery(censusData, lo, (hi+lo)/2, leftborder, bottomborder, rightborder, topborder, leftch)
			go parallelquery(censusData, (hi+lo)/2, hi, leftborder, bottomborder, rightborder, topborder, rightch)
			rightAns := <-rightch
			leftAns := <-leftch
			parent<- leftAns + rightAns;
	}
}

func parallelgrid(censusData []CensusGroup, corners Rectangle, xdim int, ydim int, lo int, hi int, parent chan [][]int) {
	if (hi - lo < SEQUENTIAL_CUTOFF) {
		grid := [][]int{}
		for i := 0; i < ydim; i++ {
			tempsplice := make([]int, xdim, xdim)
			grid = append(grid, tempsplice)
		}
		for i := lo; i < hi; i++ {
			index2 := int(((censusData[i].longitude - corners.left) / (corners.right - corners.left)) * float64(xdim))
			index1 := int(((censusData[i].latitude - corners.bottom) / (corners.top - corners.bottom)) * float64(ydim))
			if (index2 == xdim) {
				index2 = index2 - 1
			}
			if (index1 == ydim) {
				index1 = index1 - 1
			}
			grid[index1][index2] = grid[index1][index2] + censusData[i].population
		}
		parent<- grid
	} else {
			leftch := make(chan [][]int)
			rightch := make(chan [][]int)
			go parallelgrid(censusData, corners, xdim, ydim, lo, (hi+lo)/2, leftch)
			go parallelgrid(censusData, corners, xdim, ydim, (hi+lo)/2, hi, rightch)
			leftAns := <-leftch
			rightAns := <-rightch
			parent2 := make(chan [][]int)
			go parallelsum(0, 0, xdim, ydim, leftAns, rightAns, parent2)
			combinedans := <-parent2
			parent<- combinedans
	}
}

func parallelsum(lox int, loy int, hix int, hiy int, leftAns [][]int, rightAns [][]int, parent chan [][]int) {
	if ((hix-loy)*(hiy-loy) < GRID_CUTOFF) {
		for i := lox; i < hix; i++ {
			for j := loy; j < hiy; j++ {
				leftAns[j][i] = leftAns[j][i] + rightAns[j][i]
			}
		}
		parent<- leftAns
	} else {
		nech := make(chan [][]int)
		nwch := make(chan [][]int)
		sech := make(chan [][]int)
		swch := make(chan [][]int)
		go parallelsum((lox+hix)/2, (loy+hiy)/2, hix, hiy, leftAns, rightAns, nech)
		go parallelsum(lox, (loy+hiy)/2, (hix+lox)/2, hiy, leftAns, rightAns, nwch)
		go parallelsum((lox+hix)/2, loy, hix, (hiy+loy)/2, leftAns, rightAns, sech)
		go parallelsum(lox, loy, (hix+lox)/2, (hiy+loy)/2, leftAns, rightAns, swch)
		<-nech
		<-nwch
		<-sech
		<-swch
		parent<- leftAns
	}
}

func parallelgridwithlock(grid [][]int, locks [][]sync.Mutex, censusData []CensusGroup, corners Rectangle, xdim int, ydim int, lo int, hi int, parent chan [][]int) {
	if (hi - lo < SEQUENTIAL_CUTOFF) {
		for i := lo; i < hi; i++ {
			index2 := int(((censusData[i].longitude - corners.left) / (corners.right - corners.left)) * float64(xdim))
			index1 := int(((censusData[i].latitude - corners.bottom) / (corners.top - corners.bottom)) * float64(ydim))
			if (index2 == xdim) {
				index2 = index2 - 1
			}
			if (index1 == ydim) {
				index1 = index1 - 1
			}
			locks[index1][index2].Lock()
			grid[index1][index2] = grid[index1][index2] + censusData[i].population
			locks[index1][index2].Unlock()
		}
		parent<- grid
	} else {
			leftch := make(chan [][]int)
			rightch := make(chan [][]int)
			go parallelgridwithlock(grid, locks, censusData, corners, xdim, ydim, lo, (hi+lo)/2, leftch)
			go parallelgridwithlock(grid, locks, censusData, corners, xdim, ydim, (hi+lo)/2, hi, rightch)
			<-leftch
			<-rightch
			parent<- grid
	}
}
