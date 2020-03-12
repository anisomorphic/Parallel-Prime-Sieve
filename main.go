// Michael Harris
// COP4520 PA1 Problem 1
// Parallel prime sieve using segments

package main

import "os"
import "fmt"
import "time"
import "runtime"
import "math"
import "math/big"

// data structure to hold bit array
type OddBits struct {
	bits []byte
	floor int64
}

// range of primes to calculate
var min int64 = 2
var max int64 = 100000000

// how many threads to use
const num_procs int = 8

// used to track sum of primes, segment size/buffer and complete list of primes
var total_sum uint64
var seg_size int64
var seg_buf int64
var prime_list []int64

// check the last bit to determine even parity
func IsEven(a int64) bool {
	return a & 1 == 0
}

// initialize a new oddbit segment, calculate bits -> bytes needed
func CreateArray(min int64, max int64) *OddBits {
	elements := ((max - min) / 2) + 1
	data := make([]byte, (elements + 7) >> 3)

	return &OddBits{data, min}
}

// retrieve a bit from an oddbit segment from an index
func (b *OddBits) GetBit(index int64) bool {
	loc := (index - b.floor) / 2

	byte := loc >> 3
	bit := uint(loc & 0x7)

	return b.bits[byte] & (1 << bit) != 0
}

// flip a bit in an oddbit at an index
func (b *OddBits) StoreBit(index int64) {
	loc := (index - b.floor) / 2

	byte := loc >> 3
	bit := uint(loc & 0x7)

	b.bits[byte] |= 1 << bit
}

func Run(min int64, max int64) {
	// create a channel to hold our stream of primes
	primes := make(chan int64, int(seg_buf) * num_procs)
	// kick off a goroutine to generate primes first sqrt(max)
	go GenPrimes(min, max, primes)

	sum := new(big.Int)
	for p := range primes {
		// for each prime, add it to prime_list and count the sum
		prime_list = append(prime_list, p)
		sum.Add(sum, big.NewInt(p))
	}
	// update the global
	total_sum = sum.Uint64()
}

func GenPrimes(min int64, max int64, out chan<- int64) {

	// the sieve needs the primes <= sqrt(max) for the segments to reference
	var sqrtn int64 = int64(math.Sqrt(float64(max))) + 1
	prime_list_sqrtn := ArrayPrimes(sqrtn)

	if min <= 2 {
		// everything is setup to handle odds, add the only even prime if necessary
		out <- 2
	}

	for _, p := range prime_list_sqrtn {
		// only care about numbers >= min
		if p >= min {
			out <- p
		}
	}

	// the container to hold segments is a channel of channels. kick off a goroutine
	// that adds these values to the output channel of primes. this is key to parallelizing
	// but maintaining order. values are ordered inside channels, and the channels themselves are ordered.
	segments := make(chan chan int64, num_procs - 1)

	go func() {
		for s := range segments {
			for p := range s {
				out <- p
			}
		}
		// automatically closes once all channels are closed
		close(out)
	}()

	// kick off each segment. already have up to sqrt(max)
	next := sqrtn + 1
	// only care about primes >= min
	if min > next {
		next = min
	}
	for next <= max {
		// grab the window
		next_end := next + seg_size
		// don't go too far..
		if max < next_end {
			next_end = max
		}
		// create a channel, give it work, then recieve on segments
		var segchan chan int64 = make(chan int64, seg_buf)
		go GenSegment(next, next_end, prime_list_sqrtn, segchan)
		segments <- segchan

		// setup window for next iteration
		next = next_end + 1
	}
	// almost done, close segments
	close(segments)
}

// generates primes up to the square root of max
func ArrayPrimes(max int64) []int64 {
	if IsEven(max) {
		max--
	}

	primes := make([]int64, 0)
	bits := CreateArray(3, max)

	for i := int64(3); i <= max; i += 2 {
		if bits.GetBit(i) == false {
			// not divisible, prime
			primes = append(primes, i)

			// account for multiples of this prime, plus even multiples, which cannot be prime
			for multiple := i * i; multiple <= max; multiple += i * 2 {
				// store not prime
				bits.StoreBit(multiple)
			}
		}
	}
	return primes
}

// sieve primes in the window from min to max, outputting to out channel
func GenSegment(min int64, max int64, primes []int64, out chan<- int64) {
	// only looking at odd values, evens cannot be prime so adjust window slightly if necessary
	if IsEven(min) {
		min++
	}
	if IsEven(max) {
		max--
	}

	// new bit array, store the min as the floor value within the OddBits object
	bits := CreateArray(min, max)
	for _, p := range primes {
		start := p * p

		if start > max {
			// too far, skip this iteration
			break
		}


		if start < min {
			start = min - (min % p)
			// only want within bounds of min, this will ensure that happens
			if start < min {
				start += p
			}
			// only want odds
			if IsEven(start) {
				start += p
			}
		}

		// this value cannot be prime, account for all multiples within the window by marking composite
		for start <= max {
			bits.StoreBit(start)
			start += 2 * p
		}
	}

	// push all the primes out
	for i := min; i <= max; i += 2 {
		if bits.GetBit(i) == false {
			out <- i
		}
	}

	close(out)
}

func main() {
	newFile, _ := os.Create("primes.txt")

	// set 8 threads
	runtime.GOMAXPROCS(num_procs)

	// initialize size of segments (should be = sqrt(max) for optimal performance)
	// buffer size ensures access to a properly buffered channel (performance improvement ~%33)
	seg_size = int64(math.Sqrt(float64(max)))
	seg_buf = int64(math.Sqrt(float64(max))) / 5

	// calculate and show runtime, all other information
	t := time.Now()
	Run(min, max)
	fmt.Fprint(newFile, time.Since(t), len(prime_list), total_sum, "\n")

	for i := len(prime_list)-10; i < len(prime_list); i++ {
		fmt.Fprintln(newFile, prime_list[i])
	}
}
